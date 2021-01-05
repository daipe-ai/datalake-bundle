from argparse import Namespace
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TableConfigManager import TableConfigManager

class TablesOptimizerCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        tableConfigManager: TableConfigManager,
        tableExistenceChecker: TableExistenceChecker
    ):
        self.__logger = logger
        self.__spark = spark
        self.__tableConfigManager = tableConfigManager
        self.__tableExistenceChecker = tableExistenceChecker

    def getCommand(self) -> str:
        return 'datalake:table:optimize-all'

    def getDescription(self):
        return 'Runs the OPTIMIZE command on all defined tables (Delta only)'

    def run(self, inputArgs: Namespace):
        self.__logger.info('Optimizing Hive tables...')

        def filterFunc(tableConfig: TableConfig):
            return self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is True

        existingTables = self.__tableConfigManager.getByFilter(filterFunc)

        self.__logger.info(f'{len(existingTables)} tables to be optimized')

        for tableConfig in existingTables:
            self.__logger.info(f'Running OPTIMIZE {tableConfig.fullTableName}')

            self.__spark.sql(f'OPTIMIZE {tableConfig.fullTableName}')
