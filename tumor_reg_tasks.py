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
from functools import wraps
from importlib import resources as res
from typing import Iterator, Optional as Opt, Tuple
import datetime as dt
import logging

from eliot.stdlib import EliotHandler
from luigi.contrib.spark import PySparkTask
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.sql import SparkSession, functions as func
import eliot as el
import luigi

from sql_script import SQL, Environment, Params
from sql_script import SqlScript, SqlScriptError, to_qmark
import heron_load
import param_val as pv
import tumor_reg_data as td
import tumor_reg_ont as tr_ont

# Add Eliot Handler to root Logger.
logging.getLogger().addHandler(EliotHandler())
# and to luigi
logging.getLogger('luigi').addHandler(EliotHandler())
logging.getLogger('luigi-interface').addHandler(EliotHandler())
log = logging.getLogger(__name__)
el.to_file(open('log/eliot.log', 'ab'))  # ISSUE: ambient


class Connection:
    # type stubs
    def createStatement():
        pass

    def prepareStatement(sql: str):
        pass


class TheJVM:
    # Ugh... global mutable state
    __it = None
    __classpath = None

    @classmethod
    @contextmanager
    def borrow(cls, classpath):
        if cls.__it is None:
            if cls.__classpath is None:
                cls.__classpath = classpath
            else:
                assert cls.__classpath == classpath
            cls.__it = cls.__gateway(classpath)
        with el.start_action(action_type='JVM'):
            yield cls.__it

    @classmethod
    def __gateway(cls, classpath):
        from py4j.java_gateway import launch_gateway  # ISSUE: ambient

        port = launch_gateway(die_on_exit=True, classpath=classpath)
        gw = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        jvm = gw.jvm
        jvm = JavaGateway(gateway_parameters=GatewayParameters(port=port)).jvm
        return jvm

    @classmethod
    def sql_timestsamp(cls, pydt):
        with cls.borrow(cls.__classpath) as jvm:
            return jvm.java.sql.Timestamp(int(pydt.timestamp() * 1000))

    @classmethod
    def sql_date(cls, pyd):
        pydt = dt.datetime.combine(pyd, dt.datetime.min.time())
        with cls.borrow(cls.__classpath) as jvm:
            return jvm.java.sql.Date(int(pydt.timestamp() * 1000))


def with_task_logging(f):
    @wraps(f)
    def call_in_action(self, *args, **kwds):
        with el.start_task(action_type=f'{self.task_family}.{f.__name__}',
                           task_id=self.task_id,
                           **self.to_str_params(only_significant=True,
                                                only_public=True)) as ctx:
            result = f(self, *args, **kwds)
            ctx.add_success_fields(result=result)
            return result
    return call_in_action


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
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    @contextmanager
    def connection(self, action_type):
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
                err = SqlScriptError(exc, fname, line,
                                     statement, '@@TODO: connection label')
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
    def prepared(cls, conn, sql, params):
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


def _json_ok(values):
    return [v if isinstance(v, (str, int)) else str(v)
            for v in values]


class SparkJDBCTask(PySparkTask, JDBCTask):
    """Support for JDBC access from spark tasks
    """
    driver_memory = '2g'
    executor_memory = '3g'

    def output(self):
        raise NotImplementedError

    def main(self, sparkContext, *_args):
        raise NotImplementedError

    @property
    def classpath(self):
        return ':'.join(self.jars)

    def jdbc_access(self, io, table_name: str,
                    **kw_args):
        return io.jdbc(self.db_url, table_name,
                       properties={
                           "user": self.user,
                           "password": self.__password,
                           "driver": self.driver,
                       }, **kw_args)


class HelloNAACCR(SparkJDBCTask):
    """Verify connection to NAACCR ETL target DB.
    """
    schema = pv.StrParam(default='NIGHTHERONDATA')
    save_path = pv.StrParam(default='/tmp/upload_status.csv')

    def output(self):
        return luigi.LocalTarget(self.save_path)

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        upload_status = self.jdbc_access(
            spark.read, table_name=f'{self.schema}.upload_status')
        upload_status.write.save(self.output().path, format='csv')


class JDBCTableTarget(luigi.Target):
    def __init__(self, jdbc_task, query):
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
    design_id = pv.StrParam(
        default='leafs',
        description='''
        mnemonic for latest visible change to output.
        Changing this causes task_id to change, which
        ensures the ontology gets rebuilt if necessary.
        '''.strip(),
    )
    naaccr_version = pv.IntParam(default=18)
    naaccr_ddict = pv.PathParam(significant=False, description='''
      scraped from http://datadictionary.naaccr.org/default.aspx?c=10
      Content-Length: 3078052
      ISSUE: changes to the data dictionary are significant, though
             changes to the path are not. hm. checksum? cache abstraction?
    '''.strip())
    seer_recode = pv.PathParam(significant=False, description='''
      cache of http://seer.cancer.gov/siterecode/icdo3_dwhoheme/index.txt
    ''')

    table_name = "NAACCR_ONTOLOGY"  # ISSUE: parameterize? include schema name?

    @with_task_logging
    def complete(self):
        return self.output().exists()

    def output(self):
        query = f"""
          (select 1 from {self.table_name}
           where c_fullname = '\\\\i2b2\\\\naaccr\\\\'
           and c_comment = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

    @el.log_call(include_args=None)
    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)

        # oh for bind variables...
        spark.sql(f'''
          create or replace temporary view current_task as
          select "{self.task_id}" as task_id from (values('X'))
        ''')

        ont = tr_ont.NAACCR_I2B2.ont_view_in(
            spark, self.naaccr_ddict.resolve(),
            self.seer_recode.resolve())
        ont_upper = tr_ont.toDF(*[n.upper() for n in ont.columns])

        self.jdbc_access(ont_upper.write, self.table_name, mode='overwrite')


class ManualTask(luigi.Task):
    """We can check that manual tasks are complete,
    though we can't run them.
    """
    @with_task_logging
    def run(self):
        raise NotImplementedError(f'{self.task_name} is manual.')


class NAACCR_FlatFile(ManualTask):
    """A NAACCR flat file is determined by the registry, export date,
    and version.
    """
    naaccrRecordVersion = pv.IntParam(default=180)
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()
    flat_file = pv.PathParam(significant=False)

    def check_version_param(self):
        """Only version 18 (180) is currently supported.
        """
        if self.naaccrRecordVersion != 180:
            raise NotImplementedError()

    @with_task_logging
    def complete(self):
        """Check the first record, assuming all the others have
        the same export date and registry NPI.
        """
        self.check_version_param()

        with self.flat_file.open() as records:
            record0 = records.readline()

        vOk = self._checkItem(record0, 'naaccrRecordVersion',
                              str(self.naaccrRecordVersion))
        regOk = self._checkItem(record0, 'npiRegistryId',
                                self.npiRegistryId)
        dtOk = self._checkItem(record0, 'dateCaseReportExported',
                               self.dateCaseReportExported.strftime('%Y%m%d'))
        return vOk and regOk and dtOk

    @classmethod
    def _checkItem(cls, record, naaccrId, expected):
        '''
        >>> npi = '12345678901'
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

    def requires(self):
        return [NAACCR_FlatFile(
            dateCaseReportExported=self.dateCaseReportExported,
            npiRegistryId=self.npiRegistryId)]

    @with_task_logging
    def complete(self):
        return self.output().exists()

    def output(self):
        query = f"""
          (select 1 from {self.table_name}
           where task_id = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

    @el.log_call(include_args=None)
    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        [ff] = self.requires()
        naaccr_text_lines = spark.read.text(str(ff.flat_file))

        data = self._data(spark, naaccr_text_lines)
        data = data.withColumn('task_id', func.lit(self.task_id))
        data = data.toDF(*[n.upper() for n in data.columns])
        self.jdbc_access(data.write, self.table_name, mode='overwrite')

    def _data(self, spark, naaccr_text_lines):
        raise NotImplementedError('subclass must implement')


class NAACCR_Visits(_NAACCR_JDBC):
    """Make a per-tumor table for use in encounter_mapping etc.
    """
    design_id = pv.StrParam('patient_num')
    table_name = "NAACCR_TUMORS"
    encounter_num_start = pv.IntParam(description='see client.cfg')

    def _data(self, spark, naaccr_text_lines):
        tumors = td.TumorKeys.with_tumor_id(
            td.TumorKeys.pat_tmr(spark, naaccr_text_lines))
        tumors = td.TumorKeys.with_rownum(
            tumors, start=self.encounter_num_start)
        return tumors


class NAACCR_Patients(_NAACCR_JDBC):
    """Make a per-patient table for use in patient_mapping etc.
    """
    design_id = pv.StrParam('refactor dimensions')
    table_name = "NAACCR_PATIENTS"

    def _data(self, spark, naaccr_text_lines):
        patients = td.TumorKeys.patients(spark, naaccr_text_lines)
        patients = patients.withColumn('patient_num',
                                       func.lit(None).cast('int'))
        return patients


class NAACCR_Facts(_NAACCR_JDBC):
    table_name = "NAACCR_OBSERVATIONS"

    naaccr_ddict = pv.PathParam(significant=False)

    coded_view = 'tumor_coded_value'  # in
    naaccr_txform = res.read_text(heron_load, 'naaccr_txform.sql')
    fact_view = 'tumor_reg_coded_facts'  # out

    design_id = pv.StrParam('loinc map dups 3 (%d)' % len(naaccr_txform))

    def _data(self, spark, naaccr_text_lines):
        extract = td.naaccr_read_fwf(naaccr_text_lines, tr_ont.ddictDF(spark))
        ty = tr_ont.NAACCR_I2B2.tumor_item_type(spark, self.naaccr_ddict)
        obs = td.naaccr_coded_obs(extract, ty)
        obs = td.TumorKeys.with_tumor_id(obs)
        obs.createOrReplaceTempView(self.coded_view)
        tr_ont.create_object(self.fact_view, self.naaccr_txform, spark)
        data = spark.table(self.fact_view)
        return data


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

    def run_upload(self, spark, conn, upload_id):
        raise NotImplementedError('subclass must implement')

    @property
    def transform_name(self) -> str:
        return self.task_id

    @with_task_logging
    def complete(self):
        return self.output().exists()

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self, self.schema, self.source_cd)

    @with_task_logging
    def run(self):
        upload = self._upload_target()
        with upload.job(label=self.label,
                        user_id=self.user) as conn_id:
            conn, upload_id = conn_id
            self.run_upload(conn, upload_id)


class UploadTarget(luigi.Target):
    def __init__(self, jdbc_task, schema, source_cd) -> None:
        self._task = jdbc_task
        self.schema = schema
        self.source_cd = source_cd
        self.transform_name = jdbc_task.task_id

    @property
    def nextval_q(self):
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

    def insert(self, conn, label: str, user_id: str) -> int:
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
            source_cd, transform_name, load_date)
          values(:upload_id, :label, :user_id,
                 :src, :tn, current_timestamp)
        ''', dict(upload_id=upload_id,
                  label=label,
                  user_id=user_id,
                  src=self.source_cd,
                  tn=self.transform_name)) as stmt:
            rs = stmt.execute()
        return upload_id

    def update(self, conn, upload_id: int, load_status: str,
               message: Opt[str]):
        with JDBCTask.prepared(conn, f'''
          update {self.schema}.upload_status
          set end_date=current_timestamp,
              load_status = :status,
              message = :message
          where upload_id = :id
        ''', dict(status=load_status,
                  message=message,
                  id=upload_id)) as stmt:
            return stmt.executeUpdate()


def _fix_null(it, rs):
    # JDBC API for nulls is weird.
    return None if rs.wasNull() else it


class NAACCR_Load(UploadTask):
    '''Map and load NAACCR patients, tumors / visits, and facts.
    '''
    patient_ide_source = pv.StrParam(default='SMS@kumed.com')
    encounter_ide_source = pv.StrParam(default='tumor_registry@kumed.com')
    project_id = pv.StrParam(default='BlueHeron')
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()
    source_cd = pv.StrParam(default='tumor_registry@kumed.com')
    z_design_id = pv.StrParam('logging 7')
    jdbc_driver_jar = pv.StrParam(significant=False)

    script_name = 'naaccr_facts_load.sql'
    script = res.read_text(heron_load, script_name)

    @property
    def label(self) -> str:
        return self.script_name

    @property
    def classpath(self) -> str:
        return self.jdbc_driver_jar

    def requires(self):
        ff = NAACCR_FlatFile(
            dateCaseReportExported=self.dateCaseReportExported,
            npiRegistryId=self.npiRegistryId)

        return [ff] + [
            cls(db_url=self.db_url,
                user=self.user,
                passkey=self.passkey,
                dateCaseReportExported=self.dateCaseReportExported,
                npiRegistryId=self.npiRegistryId)
            for cls in [NAACCR_Patients, NAACCR_Visits, NAACCR_Facts]
        ]

    def run_upload(self, conn, upload_id):
        [ff] = self.requires()[:1]
        self.run_script(
            conn, self.script_name, self.script,
            variables=dict(upload_id=upload_id,
                           task_id=self.task_id),
            script_params=dict(upload_id=upload_id,
                               project_id=self.project_id,
                               task_id=self.task_id,
                               source_cd=self.source_cd,
                               download_date=ff.dateCaseReportExported,
                               patient_ide_source=self.patient_ide_source,
                               encounter_ide_source=self.encounter_ide_source))


if __name__ == '__main__':
    def _script_io():
        with el.start_task(action_type='luigi.build'):
            luigi.build([NAACCR_Load()], local_scheduler=True)

    _script_io()
