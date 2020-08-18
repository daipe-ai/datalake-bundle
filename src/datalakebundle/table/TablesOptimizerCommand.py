from argparse import Namespace
from logging import Logger
from pyspark.sql import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TablesConfigManager import TablesConfigManager

class TablesOptimizerCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        tablesConfigManager: TablesConfigManager,
        tableExistenceChecker: TableExistenceChecker
    ):
        self.__logger = logger
        self.__spark = spark
        self.__tablesConfigManager = tablesConfigManager
        self.__tableExistenceChecker = tableExistenceChecker

    def getCommand(self) -> str:
        return 'datalake:table:optimize-all'

    def getDescription(self):
        return 'Runs the OPTIMIZE command on all defined tables (Delta only)'

    def run(self, inputArgs: Namespace):
        self.__logger.info('Optimizing Hive tables...')

        def filterFunc(tableConfig: TableConfig):
            return self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is True

        existingTables = self.__tablesConfigManager.getByFilter(filterFunc)

        self.__logger.info('{} tables to be optimized'.format(len(existingTables)))

        for tableConfig in existingTables:
            self.__logger.info('Running OPTIMIZE {}'.format(tableConfig.fullTableName))

            self.__spark.sql('OPTIMIZE {}'.format(tableConfig.fullTableName))
