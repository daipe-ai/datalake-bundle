from pyspark.sql import SparkSession
from datalakebundle.table.TableWriter import TableWriter
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.hdfs.HdfsExists import HdfsExists

class TableCreator:

    def __init__(
        self,
        spark: SparkSession,
        tableWriter: TableWriter,
        tableExistenceChecker: TableExistenceChecker,
        hdfsExists: HdfsExists,
    ):
        self.__spark = spark
        self.__tableWriter = tableWriter
        self.__tableExistenceChecker = tableExistenceChecker
        self.__hdfsExists = hdfsExists

    def createEmptyTable(self, tableConfig: TableConfig):
        emptyDf = self.__spark.createDataFrame([], tableConfig.schema)

        if self.__tableExistenceChecker.tableExists(tableConfig.dbName, tableConfig.tableName):
            raise Exception(f'Table {tableConfig.fullTableName} already exists')

        if self.__hdfsExists.exists(tableConfig.targetPath):
            raise Exception(f'Path {tableConfig.targetPath} already exists')

        self.__tableWriter.writeIfNotExist(emptyDf, tableConfig)
