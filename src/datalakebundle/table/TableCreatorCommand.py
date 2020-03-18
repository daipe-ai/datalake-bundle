from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableCreator import TableCreator
from datalakebundle.table.TableConfig import TableConfig
from datalakebundle.table.TablesConfigManager import TablesConfigManager

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
        return 'Creates single Hive table'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='configAlias', help='Table config alias')

    def run(self, inputArgs: Namespace):
        configAlias = inputArgs.configAlias

        def filterFunc(tableConfig: TableConfig):
            return tableConfig.alias == configAlias

        configForCreations = self.__tablesConfigManager.getByFilter(filterFunc)

        if not configForCreations:
            self.__logger.error('No config found for {}. Maybe you forgot to add the data lake configuration to table.yaml?'.format(configAlias))
            return

        if len(configForCreations) > 1:
            self.__logger.error('Multiple configs found for {}'.format(configAlias))
            return

        tableConfig = configForCreations[0]

        self.__logger.info('Creating table {} for {}'.format(tableConfig.fullTableName, tableConfig.targetPath))
        self.__tableCreator.createEmptyTable(tableConfig)
