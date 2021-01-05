from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.tableActionCommand import tableActionCommand

@tableActionCommand
class TableCreatorCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tableConfigManager: TableConfigManager,
        tableCreator: TableCreator,
    ):
        self._logger = logger
        self._tableConfigManager = tableConfigManager
        self._tableCreator = tableCreator

    def getCommand(self) -> str:
        return 'datalake:table:create'

    def getDescription(self):
        return 'Creates a metastore table based on it\'s YAML definition (name, schema, data path, ...)'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        tableConfig = self._tableConfigManager.get(inputArgs.identifier)

        self._logger.info(f'Creating table {tableConfig.fullTableName} for {tableConfig.targetPath}')

        self._tableCreator.createEmptyTable(tableConfig)

        self._logger.info(f'Table {tableConfig.fullTableName} successfully created')
