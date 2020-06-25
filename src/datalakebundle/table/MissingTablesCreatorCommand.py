from argparse import Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableCreator import TableCreator
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TablesConfigManager import TablesConfigManager

class MissingTablesCreatorCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tablesConfigManager: TablesConfigManager,
        tableCreator: TableCreator,
        tableExistenceChecker: TableExistenceChecker
    ):
        self.__logger = logger
        self.__tablesConfigManager = tablesConfigManager
        self.__tableCreator = tableCreator
        self.__tableExistenceChecker = tableExistenceChecker

    def getCommand(self) -> str:
        return 'datalake:table:create-missing'

    def getDescription(self):
        return 'Creates Hive tables from app configuration'

    def run(self, inputArgs: Namespace):
        self.__logger.info('Creating Hive tables...')

        def filterFunc(tableConfig: TableConfig):
            return self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is False

        configsForCreation = self.__tablesConfigManager.getByFilter(filterFunc)

        self.__logger.info('{} tables to be created'.format(len(configsForCreation)))

        for tableConfig in configsForCreation:
            self.__logger.info('Creating table {} for {}'.format(tableConfig.fullTableName, tableConfig.targetPath))
            self.__tableCreator.createEmptyTable(tableConfig)
