from logging import Logger
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.schema.SchemaGetter import SchemaGetter
import pyspark.sql.types as T
import yaml

class TableWriter:

    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        schemaGetter: SchemaGetter,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__schemaGetter = schemaGetter

    def append(self, df: DataFrame, tableConfig: TableConfig):
        self.__save(df, tableConfig, 'append')

    def overwrite(self, df: DataFrame, tableConfig: TableConfig):
        self.__save(df, tableConfig, 'overwrite')

    def writeIfNotExist(self, df: DataFrame, tableConfig: TableConfig):
        self.__checkSchema(df, tableConfig)

        (
            df
                .write
                .partitionBy(tableConfig.partitionBy)
                .format('delta')
                .option('overwriteSchema', 'true')
                .mode('errorifexists')
                .saveAsTable(tableConfig.fullTableName, path=tableConfig.targetPath)
        )

    def __checkSchema(self, df: DataFrame, tableConfig: TableConfig):
        tableSchema = self.__schemaGetter.get(tableConfig.schemaPath)

        def printSchema(schema: T.StructType):
            return yaml.dump(schema.jsonValue())

        if tableSchema.jsonValue() != df.schema.jsonValue():
            self.__logger.warning('Table and dataframe schemas do NOT match', extra={
                'dfSchema': printSchema(df.schema),
                'tableSchema': printSchema(tableSchema),
                'tableSchemaPath': tableConfig.schemaPath,
                'table': tableConfig.fullTableName,
            })

    def __save(self, df: DataFrame, tableConfig: TableConfig, mode: str):
        self.__checkSchema(df, tableConfig)

        (
            df
                .write
                .partitionBy(tableConfig.partitionBy)
                .format('delta')
                .mode(mode)
                .saveAsTable(tableConfig.fullTableName)
        )
