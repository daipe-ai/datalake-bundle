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
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession,
        dbutils: DBUtils,
    ):
        self.__logger = logger
        self.__table_existence_checker = table_existence_checker
        self.__spark = spark
        self.__dbutils = dbutils

    def delete(self, table_config: TableConfig):
        self.__logger.warning(f"HDFS files to be deleted: {table_config.target_path}")

        if self.__table_existence_checker.table_exists(table_config.db_name, table_config.table_name) is False:
            raise UnknownTableException(f"Table {table_config.full_table_name} does not exist in Hive")

        self.drop_hive_table(table_config)
        self.delete_files(table_config)

        self.__logger.info(f"Table {table_config.full_table_name} deleted")

    def drop_hive_table(self, table_config: TableConfig):
        self.__logger.info(f"Deleting Hive table {table_config.full_table_name}")
        self.__spark.sql(f"DROP TABLE {table_config.full_table_name}")
        self.__logger.info(f"Hive table {table_config.full_table_name} deleted")

    def delete_files(self, table_config: TableConfig):
        self.__logger.info(f"Deleting HDFS files from {table_config.target_path}")
        self.__dbutils.fs.rm(table_config.target_path, True)
        self.__logger.info(f"HDFS files deleted from {table_config.target_path}")
