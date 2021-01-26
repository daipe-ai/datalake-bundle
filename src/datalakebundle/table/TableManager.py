# pylint: disable = too-many-instance-attributes
from argparse import Namespace
from logging import Logger
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.create.TableRecreator import TableRecreator
from datalakebundle.table.delete.TableDeleter import TableDeleter
from datalakebundle.table.identifier.fillTemplate import fillTemplate
from datalakebundle.table.optimize.TablesOptimizerCommand import TablesOptimizerCommand

class TableManager:

    def __init__(
        self,
        tableNameTemplate: str,
        logger: Logger,
        tableConfigManager: TableConfigManager,
        tableCreator: TableCreator,
        tableRecreator: TableRecreator,
        tableDeleter: TableDeleter,
        tableExistenceChecker: TableExistenceChecker,
        tablesOptimizerCommand: TablesOptimizerCommand,
    ):
        self.__tableNameTemplate = tableNameTemplate
        self.__logger = logger
        self.__tableConfigManager = tableConfigManager
        self.__tableCreator = tableCreator
        self.__tableRecreator = tableRecreator
        self.__tableDeleter = tableDeleter
        self.__tableExistenceChecker = tableExistenceChecker
        self.__tablesOptimizerCommand = tablesOptimizerCommand

    def getName(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        replacements = {**tableConfig.getCustomFields(), **{
            'identifier': tableConfig.identifier,
            'dbIdentifier': tableConfig.dbIdentifier,
            'tableIdentifier': tableConfig.tableIdentifier,
        }}

        return fillTemplate(self.__tableNameTemplate, replacements)

    def getConfig(self, identifier: str) -> TableConfig:
        return self.__tableConfigManager.get(identifier)

    def create(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        self.__create(tableConfig)

    def createIfNotExists(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        if self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName):
            self.__logger.info(f"Table {tableConfig.fullTableName} already exists, creation skipped")
            return

        self.__create(tableConfig)

    def recreate(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        self.__tableRecreator.recreate(tableConfig)

    def exists(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        return self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName)

    def delete(self, identifier: str):
        tableConfig = self.__tableConfigManager.get(identifier)

        self.__tableDeleter.delete(tableConfig)

        self.__logger.info(f'Table {tableConfig.fullTableName} successfully deleted')

    def optimizeAll(self):
        self.__tablesOptimizerCommand.run(Namespace())

    def __create(self, tableConfig: TableConfig):
        self.__logger.info(f'Creating table {tableConfig.fullTableName} for {tableConfig.targetPath}')

        self.__tableCreator.createEmptyTable(tableConfig)

        self.__logger.info(f'Table {tableConfig.fullTableName} successfully created')
