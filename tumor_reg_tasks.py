"""tumor_reg_tasks -- NAACCR Tumor Registry ETL Tasks

Main tasks are:
  - NAACCR_Ontology1 based on tumor_reg_ont module,
    naaaccr_concepts_load.sql
  - NAACCR_Load based on tumor_reg_data notebook/module
    and naaccr_facts_load.sql

clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py

API docs used a lot in development:
 - Package java.sql
   https://docs.oracle.com/javase/8/docs/api/index.html?java/sql/package-summary.html
"""

from abc import abstractmethod
from binascii import crc32
from contextlib import contextmanager
from importlib import resources as res
from pathlib import Path as Path_T
from typing import Any, TypeVar, Union, cast, overload
from typing import Callable, Dict, Iterator, List, Optional as Opt, Tuple
import datetime as dt
import json
import logging

from eliot.stdlib import EliotHandler
from luigi.contrib.spark import PySparkTask
from py4j.java_gateway import JavaGateway, GatewayParameters, JVMView  # type: ignore
from pyspark import SparkContext as SparkContext_T
from pyspark.sql import DataFrame, SparkSession, functions as func
import eliot as el
import luigi

from sql_script import SQL, Environment, BindValue, Params
from sql_script import SqlScript, SqlScriptError, to_qmark
import heron_load
import param_val as pv
import tumor_reg_data as td
import tumor_reg_ont as tr_ont

log = logging.getLogger(__name__)

T = TypeVar('T')


def _configure_logging(dest: Path_T) -> None:
    root = logging.getLogger()  # ISSUE: ambient
    # Add Eliot Handler to root Logger.
    root.addHandler(EliotHandler())
    # and to luigi
    logging.getLogger('luigi').addHandler(EliotHandler())
    logging.getLogger('luigi-interface').addHandler(EliotHandler())
    el.to_file(dest.open(mode='ab'))


def quiet_logs(sc: SparkContext_T) -> None:
    # ack: FDS Aug 2015 https://stackoverflow.com/a/32208445
    logger = sc._jvm.org.apache.log4j  # type: ignore
    logger.LogManager.getLogger("org"). setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)


class Timestamp:
    pass


class Date:
    pass


class ResultSet:
    def next(self) -> bool: ...
    def wasNull(self) -> bool: ...
    def getInt(self, col: str) -> int: ...


class PreparedStatement:
    """java.sql.PreparedStatement type stub
    """
    def execute(self) -> bool: ...
    def executeUpdate(self) -> int: ...
    def setNull(self, ix: int, ty: int) -> None: ...
    def setInt(self, ix: int, value: int) -> None: ...
    def setString(self, ix: int, value: str) -> None: ...
    def setDate(self, ix: int, value: Date) -> None: ...
    def close(self) -> None: ...

    @overload
    def executeQuery(self) -> ResultSet: ...
    @overload  # noqa # tell flake8 that redefinition is OK
    def executeQuery(self, sql: str) -> ResultSet: ...

    @abstractmethod  # noqa
    def executeQuery(self, sql: Opt[str] = None) -> ResultSet: pass


class Connection:
    def createStatement(self) -> PreparedStatement:
        pass

    def prepareStatement(self, sql: str) -> PreparedStatement:
        pass


class TheJVM:
    # Ugh... global mutable state
    __it = None
    __classpath = None

    @classmethod
    @contextmanager
    def borrow(cls, classpath: str) -> Iterator[JVMView]:
        if cls.__it is None:
            if cls.__classpath is None:
                cls.__classpath = classpath
            else:
                assert cls.__classpath == classpath
            cls.__it = cls.__gateway(classpath)
        with el.start_action(action_type='JVM'):
            yield cls.__it

    @classmethod
    def __gateway(cls, classpath: str) -> JVMView:
        from py4j.java_gateway import launch_gateway  # ISSUE: ambient

        port = launch_gateway(die_on_exit=True, classpath=classpath)
        gw = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        jvm = gw.jvm
        jvm = JavaGateway(gateway_parameters=GatewayParameters(port=port)).jvm
        return jvm

    @classmethod
    def sql_timestamp(cls, pydt: dt.datetime) -> Timestamp:
        assert cls.__classpath
        with cls.borrow(cls.__classpath) as jvm:
            ms = int(pydt.timestamp() * 1000)
            return cast(Timestamp, jvm.java.sql.Timestamp(ms))

    @classmethod
    def sql_date(cls, pyd: dt.date) -> Date:
        pydt = dt.datetime.combine(pyd, dt.datetime.min.time())
        assert cls.__classpath
        with cls.borrow(cls.__classpath) as jvm:
            ms = int(pydt.timestamp() * 1000)
            return cast(Date, jvm.java.sql.Date(ms))


TaskMethod = Callable[..., Any]
M = TypeVar('M', bound=TaskMethod)


@contextmanager
def task_action(task: luigi.Task, method: str) -> Iterator[el.Action]:
    with el.start_task(action_type=f'{task.task_family}.{method}',
                       task_id=task.task_id,
                       **task.to_str_params(only_significant=True,
                                            only_public=True)) as ctx:
        yield ctx


class JDBCTask(luigi.Task):
    db_url = pv.StrParam(description='see client.cfg', significant=False)
    driver = pv.StrParam(default="oracle.jdbc.OracleDriver", significant=False)
    user = pv.StrParam(description='see client.cfg', significant=False)
    passkey = pv.StrParam(description='see client.cfg',
                          significant=False)

    @property
    @abstractmethod
    def classpath(self) -> str: pass

    @property
    def __password(self) -> str:
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    @contextmanager
    def connection(self, action_type: str) -> Iterator[Connection]:
        with el.start_action(action_type=action_type,
                             url=self.db_url,
                             driver=self.driver,
                             user=self.user):
            with TheJVM.borrow(self.classpath) as jvm:
                jvm.java.lang.Class.forName(self.driver)
                conn = jvm.java.sql.DriverManager.getConnection(
                    self.db_url, self.user, self.__password)
                try:
                    yield conn
                finally:
                    conn.close()

    # ISSUE: dates are not JSON serializable, so log_call doesn't grok.
    @el.log_call(include_args=['fname', 'variables'])
    def run_script(self,
                   conn: Connection,
                   fname: str,
                   sql_code: str,
                   variables: Opt[Environment] = None,
                   script_params: Opt[Params] = None) -> None:
        '''Run script inside a LoggedConnection event.

        @param run_vars: variables to define for this run
        @param script_params: parameters to bind for this run

        To see how a script can ignore errors, see :mod:`script_lib`.
        '''
        ignore_error = False
        run_params = dict(script_params or {}, task_id=self.task_id)

        for line, _comment, statement in SqlScript.each_statement(
                sql_code, variables):
            try:
                ignore_error = self.execute_statement(
                    conn, fname, line, statement, run_params,
                    ignore_error)
            except Exception as exc:
                # ISSUE: connection label should show sid etc.
                err = SqlScriptError(exc, fname, line,
                                     statement,
                                     self.task_id)
                if ignore_error:
                    log.warning('%(event)s: %(error)s',
                                dict(event='ignore', error=err))
                else:
                    raise err from None

    @classmethod
    @el.log_call(include_args=['fname', 'line', 'ignore_error'])
    def execute_statement(cls, conn: Connection, fname: str, line: int,
                          statement: SQL, params: Params,
                          ignore_error: bool) -> bool:
        '''Log and execute one statement.
        '''
        sqlerror = SqlScript.sqlerror(statement)
        if sqlerror is not None:
            return sqlerror
        with cls.prepared(conn, statement, params) as stmt:
            stmt.execute()
        return ignore_error

    @classmethod
    @contextmanager
    def prepared(cls, conn: Connection,
                 sql: str, params: Params) -> Iterator[PreparedStatement]:
        sqlq, values = to_qmark(sql, params)
        stmt = conn.prepareStatement(sqlq)
        for ix0, value in enumerate(values):
            ix1 = ix0 + 1
            if value is None:
                VARCHAR = 12
                stmt.setNull(ix1, VARCHAR)
            if isinstance(value, int):
                stmt.setInt(ix1, value)
            elif isinstance(value, str):
                stmt.setString(ix1, value)
            elif isinstance(value, dt.date):
                dv = TheJVM.sql_date(value)
                stmt.setDate(ix1, dv)
            elif isinstance(value, dt.datetime):
                tv = TheJVM.sql_timestamp(value)
                stmt.setTimestamp(ix1, tv)
        try:
            with el.start_action(action_type='prepared statement',
                                 sql=sql.strip(), sqlq=sqlq.strip(),
                                 params=', '.join(params.keys()),
                                 values=_json_ok(values)):
                yield stmt
        finally:
            stmt.close()


def _json_ok(values: List[BindValue]) -> List[Union[int, str]]:
    return [v if isinstance(v, (str, int)) else str(v)
            for v in values]


class SparkJDBCTask(PySparkTask, JDBCTask):
    """Support for JDBC access from spark tasks
    """
    driver_memory = pv.StrParam(default='4g', significant=False)
    executor_memory = pv.StrParam(default='4g', significant=False)

    @abstractmethod
    def output(self) -> luigi.Target: pass

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.output().exists()
            ctx.add_success_fields(result=result)
            return result

    def main(self, sparkContext: SparkContext_T, *_args: Any) -> None:
        with task_action(self, 'main'):
            self.main_action(sparkContext)

    @abstractmethod
    def main_action(self, sparkContext: SparkContext_T) -> None: pass

    @property
    def classpath(self) -> str:
        return ':'.join(self.jars)

    @property
    def __password(self) -> str:
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def account(self, db_url: Opt[str] = None) -> td.Account:
        return td.Account(self.user, self.__password,
                          db_url or self.db_url, self.driver)


class HelloNAACCR(SparkJDBCTask):
    """Verify connection to NAACCR ETL target DB.
    """
    schema = pv.StrParam(default='NIGHTHERONDATA')
    save_path = pv.StrParam(default='/tmp/upload_status.csv')

    def output(self) -> luigi.Target:
        return luigi.LocalTarget(self.save_path)

    def main_action(self, sparkContext: SparkContext_T) -> None:
        spark = SparkSession(sparkContext)
        upload_status = self.account().rd(
            spark.read, table=f'{self.schema}.upload_status')
        upload_status.write.save(self.save_path, format='csv')


class JDBCTableTarget(luigi.Target):
    def __init__(self, jdbc_task: JDBCTask, query: str) -> None:
        self.query = query
        self._task = jdbc_task

    def exists(self) -> bool:
        with self._task.connection(
                f'{self.__class__.__name__}.exists') as conn:
            try:
                stmt = conn.createStatement()
                rs = stmt.executeQuery(self.query)
                return not not rs.next()  # at least one row
            except Exception:
                return False


def _stable_hash(*code: str) -> int:
    data = ('\n'.join(code)).encode('utf-8')
    return crc32(data)


class NAACCR_Ontology1(SparkJDBCTask):
    table_name = pv.StrParam(default="NAACCR_ONTOLOGY")
    who_cache = pv.PathParam()
    z_design_id = pv.StrParam(
        default='2019-09-19 recode not double %s' % _stable_hash(tr_ont.NAACCR_I2B2.ont_script.code),
        description='''
        mnemonic for latest visible change to output.
        Changing this causes task_id to change, which
        ensures the ontology gets rebuilt if necessary.
        '''.strip(),
    )
    naaccr_version = pv.IntParam(default=18)

    # based on custom_meta
    col_to_type = dict(
        c_hlevel='int',
        c_fullname='varchar(700)',
        c_name='varchar(2000)',
        c_visualattributes='varchar(3)',
        c_basecode='varchar(50)',
        c_dimcode='varchar(700)',
        c_tooltip='varchar(900)',
        update_date='date',
        sourcesystem_cd='varchar(50)',
    )
    coltypes = ','.join(
        f'{name} {ty}'
        for (name, ty) in col_to_type.items()
    )

    @property
    def version_name(self) -> str:
        """version info that fits in an i2b2 name (50 characters)
        """
        task_hash = self.task_id.split('_')[-1]  # hmm... luigi doesn't export this
        return f'v{self.naaccr_version}-{task_hash}'

    def output(self) -> JDBCTableTarget:
        query = fr"""
          (select 1 from {self.table_name}
           where c_fullname = '{tr_ont.NAACCR_I2B2.top_folder}'
           and c_basecode = '{self.version_name}')
        """
        return JDBCTableTarget(self, query)

    def main_action(self, sparkContext: SparkContext_T, *_args: Any) -> None:
        quiet_logs(sparkContext)
        spark = SparkSession(sparkContext)

        update_date = dt.datetime.strptime(self.z_design_id[:10], '%Y-%m-%d').date()
        ont = tr_ont.NAACCR_I2B2.ont_view_in(
            spark, self.version_name, who_cache=self.who_cache,
            update_date=update_date)

        self.account().wr(td.case_fold(ont).write
                          .options(createTableColumnTypes=self.coltypes),
                          self.table_name,
                          mode='overwrite')


class ManualTask(luigi.Task):
    """We can check that manual tasks are complete,
    though we can't run them.
    """
    def run(self) -> None:
        with task_action(self, 'run'):
            raise NotImplementedError(f'{self.get_task_family()} is manual.')


class NAACCR_FlatFile(ManualTask):
    """A NAACCR flat file is determined by the registry, export date,
    and version.
    """
    naaccrRecordVersion = pv.IntParam(default=180)
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()
    testData = pv.BoolParam(default=False, significant=False)
    flat_file = pv.PathParam(significant=False)
    record_qty_min = pv.IntParam(significant=False, default=1)

    def check_version_param(self) -> None:
        """Only version 18 (180) is currently supported.
        """
        if self.naaccrRecordVersion != 180:
            raise NotImplementedError()

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.complete_action()
            ctx.add_success_fields(result=result)
            return result

    def complete_action(self) -> bool:
        """Check the first record, assuming all the others have
        the same export date and registry NPI.
        """
        self.check_version_param()

        with self.flat_file.open() as records:
            record0 = records.readline()
            qty = 1 + sum(1 for _ in records.readlines())
        log.info('record qty: %d (> %d? %s)', qty,
                 self.record_qty_min, qty >= self.record_qty_min)

        vOk = self._checkItem(record0, 'naaccrRecordVersion',
                              str(self.naaccrRecordVersion))
        regOk = self._checkItem(record0, 'npiRegistryId',
                                self.npiRegistryId)
        dtOk = self._checkItem(record0, 'dateCaseReportExported',
                               self.dateCaseReportExported.strftime('%Y%m%d'))

        if vOk and regOk and dtOk and qty >= self.record_qty_min:
            return True
        else:
            if self.testData:
                log.warn('ignoring failed FlatFile check')
                return True
            return False

    @classmethod
    def _checkItem(cls, record: str, naaccrId: str, expected: str) -> bool:
        '''
        >>> npi = '1234567890'
        >>> record0 = ' ' * 19 + npi
        >>> NAACCR_FlatFile._checkItem(record0, 'npiRegistryId', npi)
        True
        >>> NAACCR_FlatFile._checkItem(record0, 'npiRegistryId', 'XXX')
        False
        '''
        itemDef = tr_ont.NAACCR1.itemDef(naaccrId)
        [startColumn, length] = [int(itemDef.attrib[it])
                                 for it in ['startColumn', 'length']]
        startColumn -= 1
        actual = record[startColumn:startColumn + length]
        if actual != expected:
            log.warn('%s: expected %s [%s:%s] = {%s} but found {%s}',
                     cls.__name__, naaccrId,
                     startColumn - 1, startColumn + length,
                     expected, actual)
        return actual == expected


class _NAACCR_JDBC(SparkJDBCTask):
    """Load data from a NAACCR flat file into a table via JDBC.

    Use a `task_id` column to manage freshness.
    """
    table_name: str
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()

    def requires(self) -> Dict[str, luigi.Task]:
        return {
            'NAACCR_FlatFile': NAACCR_FlatFile(
                dateCaseReportExported=self.dateCaseReportExported,
                npiRegistryId=self.npiRegistryId)
        }

    def _flat_file_task(self) -> NAACCR_FlatFile:
        return cast(NAACCR_FlatFile, self.requires()['NAACCR_FlatFile'])

    def output(self) -> luigi.Target:
        query = f"""
          (select 1 from {self.table_name}
           where task_id = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

    def main_action(self, sparkContext: SparkContext_T) -> None:
        quiet_logs(sparkContext)
        spark = SparkSession(sparkContext)
        ff = self._flat_file_task()
        naaccr_text_lines = spark.read.text(str(ff.flat_file))

        data = self._data(spark, naaccr_text_lines)
        # ISSUE: task_id is kinda long; how about just task_hash?
        # luigi_task_hash?
        data = data.withColumn('task_id', func.lit(self.task_id))
        data = td.case_fold(data)
        self.account().wr(data.write, self.table_name, mode='overwrite')

    def _data(self, spark: SparkSession, naaccr_text_lines: DataFrame) -> DataFrame:
        raise NotImplementedError('subclass must implement')


class NAACCR_Visits(_NAACCR_JDBC):
    """Make a per-tumor table for use in encounter_mapping etc.
    """
    design_id = pv.StrParam('patient_num')
    table_name = "NAACCR_TUMORS"
    encounter_num_start = pv.IntParam(description='see client.cfg')

    def _data(self, spark: SparkSession,
              naaccr_text_lines: DataFrame) -> DataFrame:
        tumors = td.TumorKeys.with_tumor_id(
            td.TumorKeys.pat_tmr(spark, naaccr_text_lines))
        tumors = td.TumorKeys.with_rownum(
            tumors, start=self.encounter_num_start)
        return tumors


class NAACCR_Patients(_NAACCR_JDBC):
    """Make a per-patient table for use in patient_mapping etc.
    """
    patient_ide_source = pv.StrParam(default='SMS@kumed.com')
    schema = pv.StrParam(default='NIGHTHERONDATA')
    z_design_id = pv.StrParam('keep unmapped patients')
    table_name = "NAACCR_PATIENTS"

    def requires(self) -> Dict[str, luigi.Task]:
        return dict(_NAACCR_JDBC.requires(self),
                    HERON_Patient_Mapping=HERON_Patient_Mapping(
                        patient_ide_source=self.patient_ide_source,
                        schema=self.schema,
                        db_url=self.db_url,
                        jdbc_driver_jar=self.classpath,  # KLUDGE
                        driver=self.driver,
                        user=self.user,
                        passkey=self.passkey))

    def _data(self, spark: SparkSession,
              naaccr_text_lines: DataFrame) -> DataFrame:
        patients = td.TumorKeys.patients(spark, naaccr_text_lines)
        cdw = self.account()
        patients = td.TumorKeys.with_patient_num(
            patients, spark, cdw, self.schema, self.patient_ide_source)
        return patients


class NAACCR_Facts(_NAACCR_JDBC):
    table_name = "NAACCR_OBSERVATIONS"

    z_design_id = pv.StrParam('nested fields (%s)' % _stable_hash(
        td.ItemObs.script.code,
        td.SEER_Recode.script.code,
        td.SiteSpecificFactors.script1.code,
        td.SiteSpecificFactors.script2.code))

    def _data(self, spark: SparkSession,
              naaccr_text_lines: DataFrame) -> DataFrame:
        dd = tr_ont.ddictDF(spark)
        extract = td.naaccr_read_fwf(naaccr_text_lines, dd)
        item = td.ItemObs.make(spark, extract)
        seer = td.SEER_Recode.make(spark, extract)
        ssf = td.SiteSpecificFactors.make(spark, extract)
        # ISSUE: make these separate tables?
        return item.union(seer).union(ssf)


class NAACCR_Summary(_NAACCR_JDBC):
    table_name = "NAACCR_EXPORT_STATS"

    z_design_id = pv.StrParam('fill NaN (%s)' %
                              _stable_hash(td.DataSummary.script.code))

    def _data(self, spark: SparkSession,
              naaccr_text_lines: DataFrame) -> DataFrame:
        dd = tr_ont.ddictDF(spark)
        extract = td.naaccr_read_fwf(naaccr_text_lines, dd)
        return td.DataSummary.stats(extract, spark).na.fill(0, subset=['sd'])


class UploadTask(JDBCTask):
    '''A task with an associated `upload_status` record.
    '''
    jdbc_driver_jar = pv.StrParam(significant=False)
    schema = pv.StrParam(description='owner of upload_status table')

    @property
    def classpath(self) -> str:
        return self.jdbc_driver_jar

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.output().exists()
            ctx.add_success_fields(result=result)
            return result

    def output(self) -> luigi.Target:
        return self._upload_target()

    @abstractmethod
    def _upload_target(self) -> 'UploadTarget': pass


class UploadRunTask(UploadTask):
    @property
    @abstractmethod
    def label(self) -> str: pass

    @property
    @abstractmethod
    def source_cd(self) -> str: pass

    def run(self) -> None:
        with task_action(self, 'run'):
            self.run_action()

    def run_action(self) -> None:
        upload = self._upload_target()
        with upload.job(self.label, self.user, self.source_cd) as conn_id:
            conn, upload_id = conn_id
            self.run_upload(conn, upload_id)

    @abstractmethod
    def run_upload(self, conn: Connection, upload_id: int) -> None: pass


class UploadTarget(luigi.Target):
    def __init__(self, upload_task: UploadTask,
                 schema: str, transform_name: str) -> None:
        self._task = upload_task
        self.schema = schema
        self.transform_name = transform_name

    @property
    def nextval_q(self) -> str:
        # Portability not: Oracle-only
        return f'''
          select {self.schema}.sq_uploadstatus_uploadid.nextval as value
          from dual
        '''

    def __repr__(self) -> str:
        return '%s(transform_name=%s)' % (
            self.__class__.__name__, self.transform_name)

    @el.log_call
    def exists(self) -> bool:
        q = f'''
        select max(upload_id) as upload_id
        from {self.schema}.upload_status
        where transform_name = :tn
        and load_status = 'OK'
        '''
        with self._task.connection(
                f'{self.__class__.__name__}.exists') as conn:
            with JDBCTask.prepared(conn, q,
                                   dict(tn=self.transform_name)) as stmt:
                rs = stmt.executeQuery()
                if not rs.next():
                    return False
                upload_id = _fix_null(rs.getInt('upload_id'), rs)
                if upload_id is not None:
                    log.info('UploadTarget(%s) exists: %d',
                             self.transform_name, upload_id)
        return upload_id is not None

    @contextmanager
    def job(self, label: str, user_id: str, source_cd: str) -> Iterator[
            Tuple[Connection, int]]:
        with self._task.connection(
                f'{self.__class__.__name__}.job({label})') as conn:
            upload_id = self.insert(conn, label, user_id, source_cd)

            try:
                yield conn, upload_id
            except Exception as problem:
                try:
                    self.update(conn, upload_id, 'FAILED', str(problem)[:1024])
                except Exception:
                    pass
                raise problem
            else:
                self.update(conn, upload_id, 'OK', None)

    def insert(self, conn: Connection, label: str, user_id: str, source_cd: str) -> int:
        '''
        :param label: a label for related facts for audit purposes
        :param user_id: an indication of who uploaded the related facts
        '''
        stmt = conn.createStatement()
        rs = stmt.executeQuery(self.nextval_q)
        if not rs.next():
            raise IOError('no next upload_id')
        upload_id = rs.getInt('value')  # type: int

        with JDBCTask.prepared(conn, f'''
          insert into {self.schema}.upload_status (
            upload_id, upload_label, user_id,
            source_cd, message, transform_name, load_date)
          values(:upload_id, :label, :user_id,
                 :src, :params, :tn, current_timestamp)
        ''', dict(upload_id=upload_id,
                  label=label,
                  user_id=user_id,
                  src=source_cd,
                  params=json.dumps(
                      self._task.to_str_params(only_significant=True,
                                               only_public=True),
                      indent=2)[:4000],
                  tn=self.transform_name)) as stmt:
            stmt.execute()
        return upload_id

    def update(self, conn: Connection, upload_id: int, load_status: str,
               message: Opt[str]) -> int:
        '''
        @param message: use '' to erase
        '''
        set_msg = ', message = :message ' if message else ''
        with JDBCTask.prepared(conn, f'''
          update {self.schema}.upload_status
          set end_date=current_timestamp,
              load_status = :status
              {set_msg}
          where upload_id = :id
        ''', dict(status=load_status,
                  message=message,
                  id=upload_id)) as stmt:
            return stmt.executeUpdate()


def _fix_null(it: T, rs: ResultSet) -> Opt[T]:
    # JDBC API for nulls is weird.
    return None if rs.wasNull() else it


class HERON_Patient_Mapping(UploadTask):
    patient_ide_source = pv.StrParam()
    # ISSUE: task id should depend on HERON release, too

    transform_name = 'load_epic_dimensions'

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self, self.schema, self.transform_name)

    @property
    def source_cd(self) -> str:
        return self.patient_ide_source

    def run(self) -> None:
        with task_action(self, 'run'):
            raise NotImplementedError('load_epic_dimensions is a paver task')


class _RunScriptTask(UploadRunTask):
    log_dest = pv.PathParam(significant=False)

    script_name: str

    @property
    def label(self) -> str:
        return self.script_name


class NAACCR_Load(_RunScriptTask):
    '''Map and load NAACCR patients, tumors / visits, and facts.
    '''
    # flat file attributes
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()

    # encounter mapping
    encounter_ide_source = pv.StrParam(default='tumor_registry@kumed.com')
    project_id = pv.StrParam(default='BlueHeron')
    source_cd = pv.StrParam(default='tumor_registry@kumed.com')

    # ISSUE: task_id should depend on dest schema / owner.
    z_design_id = pv.StrParam(default='nested fields')

    script_name = 'naaccr_facts_load.sql'
    script_deid_name = 'i2b2_facts_deid.sql'
    script = res.read_text(heron_load, script_name)
    script_deid = res.read_text(heron_load, script_deid_name)

    @property
    def classpath(self) -> str:
        return self.jdbc_driver_jar

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self, self.schema, transform_name=self.task_id)

    def requires(self) -> Dict[str, luigi.Task]:
        _configure_logging(self.log_dest)

        ff = NAACCR_FlatFile(
            dateCaseReportExported=self.dateCaseReportExported,
            npiRegistryId=self.npiRegistryId)

        parts = {
            cls.__name__: cls(
                db_url=self.db_url,
                user=self.user,
                passkey=self.passkey,
                dateCaseReportExported=self.dateCaseReportExported,
                npiRegistryId=self.npiRegistryId)
            for cls in [NAACCR_Patients, NAACCR_Visits, NAACCR_Facts]
        }
        return dict(parts, NAACCR_FlatFile=ff)

    def _flat_file_task(self) -> NAACCR_FlatFile:
        return cast(NAACCR_FlatFile, self.requires()['NAACCR_FlatFile'])

    def _patients_task(self) -> NAACCR_Patients:
        return cast(NAACCR_Patients, self.requires()['NAACCR_Patients'])

    def run_upload(self, conn: Connection, upload_id: int) -> None:
        ff = self._flat_file_task()
        pat = self._patients_task()

        # ISSUE: split these into separate tasks?
        for name, script in [
                (self.script_name, self.script),
                (self.script_deid_name, self.script_deid)]:
            self.run_script(
                conn, name, script,
                variables=dict(upload_id=str(upload_id),
                               task_id=self.task_id),
                script_params=dict(
                    upload_id=upload_id,
                    project_id=self.project_id,
                    task_id=self.task_id,
                    source_cd=self.source_cd,
                    download_date=ff.dateCaseReportExported,
                    patient_ide_source=pat.patient_ide_source,
                    encounter_ide_source=self.encounter_ide_source))


class NAACCR_Ontology2(_RunScriptTask):
    '''NAACCR Ontology: un-published code values

    i.e. code values that occur in tumor registry data but not in published ontologies
    '''
    # flat file attributes
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()
    source_cd = pv.StrParam(default='tumor_registry@kumed.com')

    z_design_id = pv.StrParam(default='length 50 (%s)' % _stable_hash(
        tr_ont.NAACCR_I2B2.ont_script.code,
        td.DataSummary.script.code))

    script_name = 'naaccr_concepts_mix.sql'
    script = res.read_text(heron_load, script_name)

    @property
    def classpath(self) -> str:
        return self.jdbc_driver_jar

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self, self.schema, transform_name=self.task_id)

    def requires(self) -> Dict[str, luigi.Task]:
        _configure_logging(self.log_dest)

        summary = NAACCR_Summary(
            db_url=self.db_url,
            user=self.user,
            passkey=self.passkey,
            dateCaseReportExported=self.dateCaseReportExported,
            npiRegistryId=self.npiRegistryId,
        )
        ont1 = NAACCR_Ontology1(
            db_url=self.db_url,
            user=self.user,
            passkey=self.passkey,
        )

        return dict(NAACCR_Ontology1=ont1,
                    NAACCR_Summary=summary)

    def run_upload(self, conn: Connection, upload_id: int) -> None:
        self.run_script(
            conn, self.script_name, self.script,
            variables=dict(upload_id=str(upload_id),
                           task_id=self.task_id),
            script_params=dict(
                upload_id=upload_id,
                task_id=self.task_id,
                source_cd=self.source_cd,
                update_date=self.dateCaseReportExported))


class CopyRecords(SparkJDBCTask):
    src_table = pv.StrParam()
    dest_table = pv.StrParam()
    db_url_dest = pv.StrParam()
    mode = pv.StrParam(default='error')

    # IDEA (performance): we can go parallel if we set
    # partitionColumn, lowerBound, upperBound, numPartitions
    # ref. https://stackoverflow.com/a/35062411

    def output(self) -> luigi.LocalTarget:
        """Since making a DB connection is awkward and
        scanning the desination table may be expensive,
        let's cache status in the current directory.
        """
        # ISSUE: assumes linux paths
        return luigi.LocalTarget(path=f"task_status/{self.task_id}")

    def main_action(self, sc: SparkContext_T) -> None:
        spark = SparkSession(sc)
        with el.start_action(action_type=self.get_task_family(),
                             src=self.src_table, dest=self.dest_table):
            src = self.account().rd(spark.read, self.src_table)
            with self.output().open(mode='w') as status:
                self.account(self.db_url_dest).wr(
                    src.write, self.dest_table, mode=self.mode)
                status.write(self.task_id)


class MigrateUpload(UploadRunTask):
    """
    TODO: explain of why run() is trivial, i.e. why we
    don't get an upload_status record until the end. Or fix it.
    """
    upload_id = pv.IntParam()
    workspace_schema = pv.StrParam(default='HERON_ETL_1')
    i2b2_deid = pv.StrParam(default='BlueHeronData')
    db_url_deid = pv.StrParam()
    log_dest = pv.PathParam(significant=False)

    @property
    def label(self) -> str:
        return self.get_task_family()

    @property
    def source_cd(self) -> str:
        return self.workspace_schema

    def requires(self) -> Dict[str, luigi.Task]:
        if self.complete():
            return {}

        _configure_logging(self.log_dest)
        return {
            'id': CopyRecords(
                src_table=f'{self.workspace_schema}.observation_fact_{self.upload_id}',
                dest_table=f'{self.schema}.observation_fact',
                mode='append',
                db_url=self.db_url,
                db_url_dest=self.db_url,
                driver=self.driver,
                user=self.user,
                passkey=self.passkey),
            'deid': CopyRecords(
                src_table=f'{self.workspace_schema}.observation_fact_deid_{self.upload_id}',
                dest_table=f'{self.i2b2_deid}.observation_fact',
                mode='append',
                db_url=self.db_url,
                db_url_dest=self.db_url_deid,
                driver=self.driver,
                user=self.user,
                passkey=self.passkey),
        }

    def _upload_target(self) -> UploadTarget:
        return UploadTarget(self, self.schema, transform_name=self.task_id)

    def run_upload(self, conn: Connection, upload_id: int) -> None:
        pass


def main_edit(argv: List[str], cwd: Path_T,
              value_col: str = 'value',
              exclude_pfx: str = 'reserved') -> Path_T:
    [sql_fn] = argv[1:2]

    sqlp = cwd / sql_fn
    code = sqlp.open().read()
    log.info('%s original: length %d', sqlp, len(code))
    orig = SqlScript(sql_fn, code, [])

    raw_query = td.TumorTable.fields_raw(td.TumorTable.lines_table, tr_ont.ddictDF(), value_col, exclude_pfx)
    repl = orig.replace_ddl(td.TumorTable.raw_view, raw_query)
    log.info('%s replaced %s: length %d', sqlp, td.TumorTable.raw_view, len(repl.code))

    typed_query = td.TumorTable.fields_typed(tr_ont.NAACCR_I2B2.tumor_item_type)
    repl = repl.replace_ddl(td.TumorTable.typed_view, typed_query)
    log.info('%s replaced %s: length %d', sqlp, td.TumorTable.typed_view, len(repl.code))

    for name, query in td.tumor_item_value(tr_ont.NAACCR_I2B2.tumor_item_type).items():
        is_table = not name.endswith('_all')
        repl = repl.replace_ddl(name, query, is_table=is_table)
        log.info('%s replaced %s: length %d', sqlp, name, len(repl.code))

    with sqlp.open('w') as out:
        out.write(repl.code)
    return sqlp


if __name__ == '__main__':
    def _script_io() -> None:
        from sys import argv
        from pathlib import Path

        if argv[1:] and argv[1].endswith('.sql'):
            logging.basicConfig(level=logging.INFO)
            main_edit(argv[:], Path('.'))
            return

        _configure_logging(Path('log/eliot.log'))

        with el.start_task(action_type='luigi.build'):
            luigi.build([NAACCR_Load()], local_scheduler=True)

        if False:  # static check
            MigrateUpload()

    _script_io()
