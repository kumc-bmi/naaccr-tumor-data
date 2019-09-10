"""tumor_reg_tasks -- NAACCR Tumor Registry ETL Tasks

Main tasks are:
  - NAACCR_Ontology1 based on tumor_reg_ont module,
    naaaccr_concepts_load.sql
  - NAACCR_Load based on tumor_reg_data notebook/module
    and naaccr_facts_load.sql

clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
"""

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

    def executeQuery(self, sql: Opt[str] = None) -> ResultSet:  # noqa
        raise NotImplementedError


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
    def classpath(self) -> str:
        raise NotImplementedError('subclass must implement')

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
    driver_memory = '2g'
    executor_memory = '3g'

    def output(self) -> luigi.Target:
        raise NotImplementedError

    def main(self, sparkContext: SparkContext_T, *_args: Any) -> None:
        with task_action(self, 'main'):
            self.main_action(sparkContext)

    def main_action(self, sparkContext: SparkContext_T) -> None:
        raise NotImplementedError('subclass must implement')

    @property
    def classpath(self) -> str:
        return ':'.join(self.jars)

    @property
    def __password(self) -> str:
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def account(self) -> td.Account:
        return td.Account(self.user, self.__password,
                          self.db_url, self.driver)


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


class NAACCR_Ontology1(SparkJDBCTask):
    z_design_id = pv.StrParam(
        default='create_objects %s' % hash(tr_ont.NAACCR_I2B2.ont_script.code),
        description='''
        mnemonic for latest visible change to output.
        Changing this causes task_id to change, which
        ensures the ontology gets rebuilt if necessary.
        '''.strip(),
    )
    naaccr_version = pv.IntParam(default=18)
    seer_recode = pv.PathParam(default=None, description='''
      cache of http://seer.cancer.gov/siterecode/icdo3_dwhoheme/index.txt
    ''')

    table_name = "NAACCR_ONTOLOGY"  # ISSUE: parameterize? include schema name?

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.output().exists()
            ctx.add_success_fields(result=result)
            return result

    def output(self) -> JDBCTableTarget:
        query = f"""
          (select 1 from {self.table_name}
           where c_fullname = '\\\\i2b2\\\\naaccr\\\\'
           and c_comment = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

    def main_action(self, sparkContext: SparkContext_T, *_args: Any) -> None:
        quiet_logs(sparkContext)
        spark = SparkSession(sparkContext)

        # oh for bind variables...
        spark.sql(f'''
          create or replace temporary view current_task as
          select "{self.task_id}" as task_id from (values('X'))
        ''')

        ont = tr_ont.NAACCR_I2B2.ont_view_in(
            spark, self.seer_recode and self.seer_recode.resolve())

        self.account().wr(td.case_fold(ont).write, self.table_name,
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

        return vOk and regOk and dtOk and qty >= self.record_qty_min

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

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.output().exists()
            ctx.add_success_fields(result=result)
            return result

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
                        classpath=self.classpath,
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

    z_design_id = pv.StrParam('with seer, ssf; (%s)' % hash(
        (td.ItemObs.script,
         td.SEER_Recode.script,
         td.SiteSpecificFactors.script1,
         td.SiteSpecificFactors.script2)))

    def _data(self, spark: SparkSession,
              naaccr_text_lines: DataFrame) -> DataFrame:
        dd = tr_ont.ddictDF(spark)
        extract = td.naaccr_read_fwf(naaccr_text_lines, dd)
        item = td.ItemObs.make(spark, extract)
        seer = td.SEER_Recode.make(spark, extract)
        ssf = td.SiteSpecificFactors.make(spark, extract)
        # ISSUE: make these separate tables?
        return item.union(seer).union(ssf)


class UploadTask(JDBCTask):
    '''A task with an associated `upload_status` record.
    '''
    schema = pv.StrParam(description='@@TODO: see client.cfg')

    @property
    def source_cd(self) -> str:
        raise NotImplementedError('subclass must implement')

    @property
    def label(self) -> str:
        raise NotImplementedError('subclass must implement')

    def run_upload(self, conn: Connection, upload_id: int) -> None:
        raise NotImplementedError('subclass must implement')

    @property
    def transform_name(self) -> str:
        return self.task_id

    def complete(self) -> bool:
        with task_action(self, 'complete') as ctx:
            result = self.output().exists()
            ctx.add_success_fields(result=result)
            return result

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self, self.schema, self.source_cd)

    def run(self) -> None:
        with task_action(self, 'run'):
            self.run_action()

    def run_action(self) -> None:
        upload = self._upload_target()
        with upload.job(label=self.label,
                        user_id=self.user) as conn_id:
            conn, upload_id = conn_id
            self.run_upload(conn, upload_id)


class UploadTarget(luigi.Target):
    def __init__(self, upload_task: UploadTask,
                 schema: str, source_cd: str) -> None:
        self._task = upload_task
        self.schema = schema
        self.source_cd = source_cd
        self.transform_name = upload_task.transform_name

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
    def job(self, label: str, user_id: str) -> Iterator[
            Tuple[Connection, int]]:
        with self._task.connection(
                f'{self.__class__.__name__}.job({label})') as conn:
            upload_id = self.insert(conn, label, user_id)

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

    def insert(self, conn: Connection, label: str, user_id: str) -> int:
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
                  src=self.source_cd,
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
    classpath = pv.StrParam(significant=False)

    @property
    def transform_name(self) -> str:
        return 'load_epic_dimensions'

    @property
    def source_cd(self) -> str:
        return self.patient_ide_source

    @property
    def label(self) -> str:
        raise NotImplementedError(self)

    def run(self) -> None:
        raise NotImplementedError('load_epic_dimensions is a paver task')


class NAACCR_Load(UploadTask):
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
    z_design_id = pv.StrParam(default='record_layout num join')

    jdbc_driver_jar = pv.StrParam(significant=False)
    log_dest = pv.PathParam(significant=False)

    script_name = 'naaccr_facts_load.sql'
    script_deid_name = 'i2b2_facts_deid.sql'
    script = res.read_text(heron_load, script_name)
    script_deid = res.read_text(heron_load, script_deid_name)

    @property
    def label(self) -> str:
        return self.script_name

    @property
    def classpath(self) -> str:
        return self.jdbc_driver_jar

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


if __name__ == '__main__':
    def _script_io() -> None:
        from pathlib import Path

        _configure_logging(Path('log/eliot.log'))

        with el.start_task(action_type='luigi.build'):
            luigi.build([NAACCR_Load()], local_scheduler=True)

    _script_io()
