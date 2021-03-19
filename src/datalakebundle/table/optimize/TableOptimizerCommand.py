from argparse import Namespace, ArgumentParser
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.table_action_command import table_action_command


@table_action_command
class TableOptimizerCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_config_manager: TableConfigManager,
    ):
        self._logger = logger
        self._spark = spark
        self._table_config_manager = table_config_manager

    def get_command(self) -> str:
        return "datalake:table:optimize"

    def get_description(self):
        return "OPTIMIZEs given table"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")

    def run(self, input_args: Namespace):
        table_config = self._table_config_manager.get(input_args.identifier)

        self._logger.info(f"Running OPTIMIZE {table_config.full_table_name}")

        self._spark.sql(f"OPTIMIZE {table_config.full_table_name}")

        self._logger.info(f"OPTIMIZE for {table_config.full_table_name} completed")
