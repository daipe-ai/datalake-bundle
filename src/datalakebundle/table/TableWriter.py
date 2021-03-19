from logging import Logger
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.config.TableConfig import TableConfig
import pyspark.sql.types as t
import yaml


class TableWriter:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
    ):
        self.__logger = logger
        self.__spark = spark

    def append(self, df: DataFrame, table_config: TableConfig):
        self.__save(df, table_config, "append")

    def overwrite(self, df: DataFrame, table_config: TableConfig):
        self.__save(df, table_config, "overwrite")

    def write_if_not_exist(self, df: DataFrame, table_config: TableConfig):
        self.__check_schema(df, table_config)

        (
            df.write.partitionBy(table_config.partition_by)
            .format("delta")
            .option("overwriteSchema", "true")
            .mode("errorifexists")
            .saveAsTable(table_config.full_table_name, path=table_config.target_path)
        )

    def __check_schema(self, df: DataFrame, table_config: TableConfig):
        table_schema = table_config.schema

        def print_schema(schema: t.StructType):
            return yaml.dump(schema.jsonValue())

        if table_schema.jsonValue() != df.schema.jsonValue():
            self.__logger.warning(
                "Table and dataframe schemas do NOT match",
                extra={
                    "df_schema": print_schema(df.schema),
                    "table_schema": print_schema(table_schema),
                    "table_schema_loader": table_config.schema_loader,
                    "table": table_config.full_table_name,
                },
            )

    def __save(self, df: DataFrame, table_config: TableConfig, mode: str):
        self.__check_schema(df, table_config)

        (df.write.partitionBy(table_config.partition_by).format("delta").mode(mode).saveAsTable(table_config.full_table_name))
