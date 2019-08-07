'''tumor_reg_tasks -- NAACCR Tumor Registry ETL Tasks

clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
'''

from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import luigi

import param_val as pv


class HelloNAACCR(PySparkTask):
    '''Verify connection to NAACCR ETL target DB.
    '''
    driver_memory = '2g'
    executor_memory = '3g'

    schema = pv.StrParam(default='NIGHTHERONDATA')
    save_path = pv.StrParam(default='/tmp/upload_status.csv')

    db_url = pv.StrParam(description='see client.cfg')
    driver = pv.StrParam(default="oracle.jdbc.OracleDriver")
    user = pv.StrParam(description='see client.cfg')
    passkey = pv.StrParam(description='see client.cfg',
                          significant=False)

    @property
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def output(self):
        return luigi.LocalTarget(self.save_path)

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        upload_status = spark.read.jdbc(
            table='{t.schema}.upload_status'.format(t=self),
            url=self.db_url,
            properties={
                "user": self.user,
                "password": self.__password,
                "driver": self.driver,
            }).limit(10)
        upload_status.write.save(self.output().path, format='csv')
