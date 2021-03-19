from logging import Logger
from datalakebundle.hdfs.HdfsExists import HdfsExists
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.delete.TableDeleter import TableDeleter


class TableRecreator:
    def __init__(
        self,
        logger: Logger,
        table_creator: TableCreator,
        table_existence_checker: TableExistenceChecker,
        table_deleter: TableDeleter,
        hdfs_exists: HdfsExists,
    ):
        self.__logger = logger
        self.__table_creator = table_creator
        self.__table_existence_checker = table_existence_checker
        self.__table_deleter = table_deleter
        self.__hdfs_exists = hdfs_exists

    def recreate(self, table_config: TableConfig):
        if self.__table_existence_checker.table_exists(table_config.db_name, table_config.table_name):
            self.__recreate_hive_table(table_config)
        elif self.__hdfs_exists.exists(table_config.target_path):
            self.__create_new_table_when_data_existed(table_config)
        else:
            self.__logger.info(f"Creating new Hive table {table_config.full_table_name} (didn't exist before)")
            self.__table_creator.create_empty_table(table_config)

    def __recreate_hive_table(self, table_config: TableConfig):
        self.__table_deleter.drop_hive_table(table_config)

        if not self.__hdfs_exists.exists(table_config.target_path):
            self.__logger.warning(f"No files in {table_config.target_path} for existing Hive table {table_config.full_table_name}")
        else:
            self.__table_deleter.delete_files(table_config)

        self.__logger.info(f"Recreating Hive table {table_config.full_table_name} (existed before)")
        self.__table_creator.create_empty_table(table_config)

    def __create_new_table_when_data_existed(self, table_config: TableConfig):
        self.__logger.warning(f"Hive table {table_config.full_table_name} does NOT exists for existing files in {table_config.target_path}")

        self.__table_deleter.delete_files(table_config)

        self.__logger.info(f"Creating new Hive table {table_config.full_table_name} (data existed before)")
        self.__table_creator.create_empty_table(table_config)
