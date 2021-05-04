import sys
from argparse import Namespace, ArgumentParser
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.parameters.TableParametersManager import TableParametersManager


class TableOptimizerCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_parameters_manager: TableParametersManager,
        table_existence_checker: TableExistenceChecker,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_parameters_manager = table_parameters_manager
        self.__table_existence_checker = table_existence_checker

    def get_command(self) -> str:
        return "datalake:table:optimize"

    def get_description(self):
        return "OPTIMIZEs given table"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")

    def run(self, input_args: Namespace):
        table_parameters = self.__table_parameters_manager.get_or_parse(input_args.identifier)

        if not self.__table_existence_checker.table_exists(table_parameters.db_name, table_parameters.table_name):
            self.__logger.error(f"Hive table {table_parameters.full_table_name} does NOT exists")
            sys.exit(1)

        self.__logger.info(f"Running OPTIMIZE {table_parameters.full_table_name}")

        self.__spark.sql(f"OPTIMIZE {table_parameters.full_table_name}")

        self.__logger.info(f"OPTIMIZE for {table_parameters.full_table_name} completed")
