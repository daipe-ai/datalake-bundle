from pyspark.sql.session import SparkSession

class HdfsExists:

    def __init__(
        self,
        spark: SparkSession,
    ):
        self.__spark = spark

    def exists(self, filePath: str):
        jvm = self.__spark._jvm  # pylint: disable = protected-access
        jsc = self.__spark._jsc  # pylint: disable = protected-access
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

        return fs.exists(jvm.org.apache.hadoop.fs.Path(filePath))
