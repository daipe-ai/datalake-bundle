from argparse import ArgumentParser, Namespace
from logging import Logger
from pyspark.sql.session import SparkSession
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.hdfs.HdfsDelete import HdfsDelete
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.config.TablesConfigManager import TablesConfigManager

class TableDeleterCommand(ConsoleCommand):

    def __init__(
        self,
        logger: Logger,
        tablesConfigManager: TablesConfigManager,
        tableExistenceChecker: TableExistenceChecker,
        spark: SparkSession,
        hdfsDelete: HdfsDelete,
    ):
        self.__logger = logger
        self.__tablesConfigManager = tablesConfigManager
        self.__tableExistenceChecker = tableExistenceChecker
        self.__spark = spark
        self.__hdfsDelete = hdfsDelete

    def getCommand(self) -> str:
        return 'datalake:table:delete'

    def getDescription(self):
        return 'Deletes a metastore table including data on HDFS'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Table identifier')

    def run(self, inputArgs: Namespace):
        identifier = inputArgs.identifier

        tableConfig = self.__tablesConfigManager.get(identifier)

        if not tableConfig:
            self.__logger.error('Table {} not found in config'.format(identifier))
            return

        self.__logger.warning('HDFS files to be deleted: {}'.format(tableConfig.targetPath))

        if self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName) is False:
            self.__logger.error('Table {} does not exist in Hive'.format(identifier))
            return

        self.__logger.info('Deleting Hive table {}'.format(identifier))
        self.__spark.sql('DROP TABLE {}'.format(tableConfig.fullTableName))

        self.__logger.info('Deleting HDFS files from {}'.format(tableConfig.targetPath))
        self.__hdfsDelete.delete(tableConfig.targetPath, True)

        self.__logger.info('Table {} deleted'.format(identifier))
