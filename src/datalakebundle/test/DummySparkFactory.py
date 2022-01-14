# pylint: disable = super-init-not-called
from pyspark.sql import SparkSession


class DummySparkFactory:
    def create(self):
        class DummySparkSession(SparkSession):
            def __init__(self, *args):
                pass

        return DummySparkSession()
