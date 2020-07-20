import luigi


class PySparkTask(luigi.Task):
    jars: str

    def main(self, sparkContext: SparkContext_T, *_args: Any) -> None:
        raise NotImplementedError
