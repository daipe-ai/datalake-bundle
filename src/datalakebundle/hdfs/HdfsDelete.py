from pyspark.sql.session import SparkSession

class HdfsDelete:

    def __init__(
        self,
        spark: SparkSession,
    ):
        self.__spark = spark

    def delete(self, filePath: str, recursive: bool = False):
        jvm = self.__spark._jvm  # pylint: disable = protected-access
        jsc = self.__spark._jsc  # pylint: disable = protected-access
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

        return fs.delete(jvm.org.apache.hadoop.fs.Path(filePath), recursive)
