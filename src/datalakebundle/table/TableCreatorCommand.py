from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableCreator import TableCreator
from datalakebundle.table.config.TablesConfigManager import TablesConfigManager

class TableCreatorCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tablesConfigManager: TablesConfigManager,
        tableCreator: TableCreator,
    ):
        self.__logger = logger
        self.__tablesConfigManager = tablesConfigManager
        self.__tableCreator = tableCreator

    def getCommand(self) -> str:
        return 'datalake:table:create'

    def getDescription(self):
        return 'Creates a metastore table based on it\'s YAML definition (name, schema, data path, ...)'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        tableConfig = self.__tablesConfigManager.get(inputArgs.identifier)

        if not tableConfig:
            self.__logger.error('No config found for {}. Maybe you forgot to add the data lake configuration to table.yaml?'.format(inputArgs.identifier))
            return

        self.__logger.info('Creating table {} for {}'.format(tableConfig.fullTableName, tableConfig.targetPath))
        self.__tableCreator.createEmptyTable(tableConfig)
