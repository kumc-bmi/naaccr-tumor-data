"""tumor_reg_tasks -- NAACCR Tumor Registry ETL Tasks

clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
"""

from importlib import resources as res
from contextlib import contextmanager
import logging

from luigi.contrib.spark import PySparkTask
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.sql import SparkSession, functions as func
import luigi

import tumor_reg_data as td
import tumor_reg_ont as tr_ont
import heron_load
import param_val as pv

log = logging.getLogger(__name__)


class SparkJDBCTask(PySparkTask):
    """Support for JDBC access from spark tasks
    """
    driver_memory = '2g'
    executor_memory = '3g'

    db_url = pv.StrParam(description='see client.cfg', significant=False)
    driver = pv.StrParam(default="oracle.jdbc.OracleDriver", significant=False)
    user = pv.StrParam(description='see client.cfg', significant=False)
    passkey = pv.StrParam(description='see client.cfg',
                          significant=False)

    @property
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def output(self):
        raise NotImplementedError

    def main(self, sparkContext, *_args):
        raise NotImplementedError

    def jdbc_access(self, io, table_name: str,
                    **kw_args):
        return io.jdbc(self.db_url, table_name,
                       properties={
                           "user": self.user,
                           "password": self.__password,
                           "driver": self.driver,
                       }, **kw_args)

    @contextmanager
    def connection(self):
        from py4j.java_gateway import launch_gateway  # ISSUE: ambient

        port = launch_gateway(die_on_exit=True, classpath=':'.join(self.jars))
        gw = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        jvm = gw.jvm
        jvm = JavaGateway(gateway_parameters=GatewayParameters(port=port)).jvm
        jvm.java.lang.Class.forName(self.driver)
        conn = jvm.java.sql.DriverManager.getConnection(
            self.db_url, self.user, self.__password)
        try:
            yield conn
        finally:
            conn.close()
            gw.shutdown()


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
        with self._task.connection() as conn:
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

    def output(self):
        query = f"""
          (select 1 from {self.table_name}
           where c_fullname = '\\\\i2b2\\\\naaccr\\\\'
           and c_comment = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

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


class NAACCR_FlatFile(luigi.Task):
    naaccrRecordVersion = pv.IntParam(default=180)
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()
    flat_file = pv.PathParam(significant=False)

    def complete(self):
        if self.naaccrRecordVersion != 180:
            raise NotImplementedError()

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

    def run(self):
        raise NotImplementedError('NAACCR flat file staging is manual.')


class _NAACCR_JDBC(SparkJDBCTask):
    table_name: str
    dateCaseReportExported = pv.DateParam()
    npiRegistryId = pv.StrParam()

    def requires(self):
        return [NAACCR_FlatFile(
            dateCaseReportExported=self.dateCaseReportExported,
            npiRegistryId=self.npiRegistryId)]

    def output(self):
        query = f"""
          (select 1 from {self.table_name}
           where task_id = '{self.task_id}')
        """
        return JDBCTableTarget(self, query)

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
