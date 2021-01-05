from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.create.TableRecreator import TableRecreator
from datalakebundle.table.tableActionCommand import tableActionCommand

@tableActionCommand
class TableRecreatorCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tableConfigManager: TableConfigManager,
        tableRecreator: TableRecreator,
    ):
        self._logger = logger
        self._tableConfigManager = tableConfigManager
        self._tableRecreator = tableRecreator

    def getCommand(self) -> str:
        return 'datalake:table:recreate'

    def getDescription(self):
        return 'Recreates a metastore table based on it\'s YAML definition (name, schema, data path, ...)'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        tableConfig = self._tableConfigManager.get(inputArgs.identifier)

        self._tableRecreator.recreate(tableConfig)
