from logging import Logger
from pyspark.dbutils import DBUtils
from pyspark.sql.session import SparkSession
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.UnknownTableException import UnknownTableException
from datalakebundle.table.config.TableConfig import TableConfig

class TableDeleter:

    def __init__(
        self,
        logger: Logger,
        tableExistenceChecker: TableExistenceChecker,
        spark: SparkSession,
        dbutils: DBUtils,
    ):
        self.__logger = logger
        self.__tableExistenceChecker = tableExistenceChecker
        self.__spark = spark
        self.__dbutils = dbutils

    def delete(self, tableConfig: TableConfig):
        self.__logger.warning(f'HDFS files to be deleted: {tableConfig.targetPath}')

        if self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is False:
            raise UnknownTableException(f'Table {tableConfig.fullTableName} does not exist in Hive')

        self.dropHiveTable(tableConfig)
        self.deleteFiles(tableConfig)

        self.__logger.info(f'Table {tableConfig.fullTableName} deleted')

    def dropHiveTable(self, tableConfig: TableConfig):
        self.__logger.info(f'Deleting Hive table {tableConfig.fullTableName}')
        self.__spark.sql(f'DROP TABLE {tableConfig.fullTableName}')
        self.__logger.info(f'Hive table {tableConfig.fullTableName} deleted')

    def deleteFiles(self, tableConfig: TableConfig):
        self.__logger.info(f'Deleting HDFS files from {tableConfig.targetPath}')
        self.__dbutils.fs.rm(tableConfig.targetPath, True)
        self.__logger.info(f'HDFS files deleted from {tableConfig.targetPath}')
