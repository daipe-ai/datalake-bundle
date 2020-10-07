from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper

# @deprecated, use dbutils.fs.rm() instead
class HdfsDelete:

    def __init__(
        self,
        dbutils: DbUtilsWrapper,
    ):
        self.__dbutils = dbutils

    def delete(self, filePath: str, recursive: bool = False):
        self.__dbutils.fs.rm(filePath, recursive)
