from argparse import Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TableConfigManager import TableConfigManager


class MissingTablesCreatorCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_config_manager: TableConfigManager,
        table_creator: TableCreator,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__table_config_manager = table_config_manager
        self.__table_creator = table_creator
        self.__table_existence_checker = table_existence_checker

    def get_command(self) -> str:
        return "datalake:table:create-missing"

    def get_description(self):
        return "Creates newly defined tables that do not exist in the metastore yet"

    def run(self, input_args: Namespace):
        self.__logger.info("Creating Hive tables...")

        def filter_func(table_config: TableConfig):
            return self.__table_existence_checker.table_exists(table_config.db_name, table_config.table_name) is False

        configs_for_creation = self.__table_config_manager.get_by_filter(filter_func)

        self.__logger.info(f"{len(configs_for_creation)} tables to be created")

        for table_config in configs_for_creation:
            self.__logger.info(f"Creating table {table_config.fullTableName} for {table_config.targetPath}")
            self.__table_creator.create_empty_table(table_config)
