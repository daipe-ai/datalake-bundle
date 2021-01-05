import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.UnknownTableException import UnknownTableException
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.delete.TableDeleter import TableDeleter
from datalakebundle.table.tableActionCommand import tableActionCommand

@tableActionCommand
class TableDeleterCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tableConfigManager: TableConfigManager,
        tableDeleter: TableDeleter,
    ):
        self._logger = logger
        self._tableConfigManager = tableConfigManager
        self._tableDeleter = tableDeleter

    def getCommand(self) -> str:
        return 'datalake:table:delete'

    def getDescription(self):
        return 'Deletes a metastore table including data on HDFS'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        tableConfig = self._tableConfigManager.get(inputArgs.identifier)

        try:
            self._tableDeleter.delete(tableConfig)

            self._logger.info(f'Table {tableConfig.fullTableName} successfully deleted')
        except UnknownTableException as e:
            self._logger.error(str(e))
            sys.exit(1)
