from argparse import Namespace, ArgumentParser
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.tableActionCommand import tableActionCommand

@tableActionCommand
class TableOptimizerCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        tableConfigManager: TableConfigManager,
    ):
        self._logger = logger
        self._spark = spark
        self._tableConfigManager = tableConfigManager

    def getCommand(self) -> str:
        return 'datalake:table:optimize'

    def getDescription(self):
        return 'OPTIMIZEs given table'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        tableConfig = self._tableConfigManager.get(inputArgs.identifier)

        self._logger.info(f'Running OPTIMIZE {tableConfig.fullTableName}')

        self._spark.sql(f'OPTIMIZE {tableConfig.fullTableName}')

        self._logger.info(f'OPTIMIZE for {tableConfig.fullTableName} completed')
