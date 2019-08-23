"""tumor_reg_tasks -- NAACCR Tumor Registry ETL Tasks

clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
"""

from contextlib import contextmanager
import logging

from luigi.contrib.spark import PySparkTask
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.sql import SparkSession
import luigi

import param_val as pv
from tumor_reg_ont import NAACCR_I2B2

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

    def exists(self):
        with self._task.connection() as conn:
            try:
                stmt = conn.createStatement()
                rs = stmt.executeQuery(self.query)
                return rs.next()  # at least one row
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
    naaccr_ch10_bytes = pv.IntParam(default=3078052, description='''
      Content-Length of http://datadictionary.naaccr.org/default.aspx?c=10
    '''.strip())
    naaccr_ddict = pv.PathParam(significant=False)

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

        ont = NAACCR_I2B2.ont_view_in(spark, self.naaccr_ddict.resolve())
        ont_upper = ont.toDF(*[n.upper() for n in ont.columns])
        self.jdbc_access(ont_upper.write, self.table_name, mode='overwrite')
