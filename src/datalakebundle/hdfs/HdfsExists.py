from pyspark.dbutils import DBUtils

class HdfsExists:

    def __init__(
        self,
        dbutils: DBUtils,
    ):
        self.__dbutils = dbutils

    def exists(self, path: str):
        try:
            self.__dbutils.fs.head(path)

            return True
        except Exception as e:
            if 'Cannot head a directory:' in str(e):
                return True

            if 'java.io.FileNotFoundException' in str(e):
                return False

            raise
