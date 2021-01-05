from logging import Logger
from datalakebundle.hdfs.HdfsExists import HdfsExists
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.delete.TableDeleter import TableDeleter

class TableRecreator:

    def __init__(
        self,
        logger: Logger,
        tableCreator: TableCreator,
        tableExistenceChecker: TableExistenceChecker,
        tableDeleter: TableDeleter,
        hdfsExists: HdfsExists,
    ):
        self.__logger = logger
        self.__tableCreator = tableCreator
        self.__tableExistenceChecker = tableExistenceChecker
        self.__tableDeleter = tableDeleter
        self.__hdfsExists = hdfsExists

    def recreate(self, tableConfig: TableConfig):
        if self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName):
            self.__recreateHiveTable(tableConfig)
        elif self.__hdfsExists.exists(tableConfig.targetPath):
            self.__createNewTableWhenDataExisted(tableConfig)
        else:
            self.__logger.info(f'Creating new Hive table {tableConfig.fullTableName} (didn\'t exist before)')
            self.__tableCreator.createEmptyTable(tableConfig)

    def __recreateHiveTable(self, tableConfig: TableConfig):
        self.__tableDeleter.dropHiveTable(tableConfig)

        if not self.__hdfsExists.exists(tableConfig.targetPath):
            self.__logger.warning(f'No files in {tableConfig.targetPath} for existing Hive table {tableConfig.fullTableName}')
        else:
            self.__tableDeleter.deleteFiles(tableConfig)

        self.__logger.info(f'Recreating Hive table {tableConfig.fullTableName} (existed before)')
        self.__tableCreator.createEmptyTable(tableConfig)

    def __createNewTableWhenDataExisted(self, tableConfig: TableConfig):
        self.__logger.warning(f'Hive table {tableConfig.fullTableName} does NOT exists for existing files in {tableConfig.targetPath}')

        self.__tableDeleter.deleteFiles(tableConfig)

        self.__logger.info(f'Creating new Hive table {tableConfig.fullTableName} (data existed before)')
        self.__tableCreator.createEmptyTable(tableConfig)
