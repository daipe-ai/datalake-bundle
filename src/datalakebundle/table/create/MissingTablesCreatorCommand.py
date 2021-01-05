from argparse import Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TableConfigManager import TableConfigManager

class MissingTablesCreatorCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tableConfigManager: TableConfigManager,
        tableCreator: TableCreator,
        tableExistenceChecker: TableExistenceChecker
    ):
        self.__logger = logger
        self.__tableConfigManager = tableConfigManager
        self.__tableCreator = tableCreator
        self.__tableExistenceChecker = tableExistenceChecker

    def getCommand(self) -> str:
        return 'datalake:table:create-missing'

    def getDescription(self):
        return 'Creates newly defined tables that do not exist in the metastore yet'

    def run(self, inputArgs: Namespace):
        self.__logger.info('Creating Hive tables...')

        def filterFunc(tableConfig: TableConfig):
            return self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is False

        configsForCreation = self.__tableConfigManager.getByFilter(filterFunc)

        self.__logger.info(f'{len(configsForCreation)} tables to be created')

        for tableConfig in configsForCreation:
            self.__logger.info(f'Creating table {tableConfig.fullTableName} for {tableConfig.targetPath}')
            self.__tableCreator.createEmptyTable(tableConfig)
