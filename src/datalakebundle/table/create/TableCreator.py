from pyspark.sql import SparkSession
from datalakebundle.table.TableWriter import TableWriter
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.hdfs.HdfsExists import HdfsExists


class TableCreator:
    def __init__(
        self,
        spark: SparkSession,
        table_writer: TableWriter,
        table_existence_checker: TableExistenceChecker,
        hdfs_exists: HdfsExists,
    ):
        self.__spark = spark
        self.__table_writer = table_writer
        self.__table_existence_checker = table_existence_checker
        self.__hdfs_exists = hdfs_exists

    def create_empty_table(self, table_config: TableConfig):
        empty_df = self.__spark.createDataFrame([], table_config.schema)

        if self.__table_existence_checker.table_exists(table_config.db_name, table_config.table_name):
            raise Exception(f"Table {table_config.fullTableName} already exists")

        if self.__hdfs_exists.exists(table_config.target_path):
            raise Exception(f"Path {table_config.target_path} already exists")

        self.__table_writer.write_if_not_exist(empty_df, table_config)
