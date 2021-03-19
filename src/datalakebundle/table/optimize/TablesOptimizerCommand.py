from argparse import Namespace
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TableConfigManager import TableConfigManager


class TablesOptimizerCommand(ConsoleCommand):
    def __init__(
        self, logger: Logger, spark: SparkSession, table_config_manager: TableConfigManager, table_existence_checker: TableExistenceChecker
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_config_manager = table_config_manager
        self.__table_existence_checker = table_existence_checker

    def get_command(self) -> str:
        return "datalake:table:optimize-all"

    def get_description(self):
        return "Runs the OPTIMIZE command on all defined tables (Delta only)"

    def run(self, input_args: Namespace):
        self.__logger.info("Optimizing Hive tables...")

        def filter_func(table_config: TableConfig):
            return self.__table_existence_checker.table_exists(table_config.db_name, table_config.table_name) is True

        existing_tables = self.__table_config_manager.get_by_filter(filter_func)

        self.__logger.info(f"{len(existing_tables)} tables to be optimized")

        for table_config in existing_tables:
            self.__logger.info(f"Running OPTIMIZE {table_config.full_table_name}")

            self.__spark.sql(f"OPTIMIZE {table_config.full_table_name}")
