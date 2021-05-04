import string
import random
from logging import Logger
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from datalakebundle.table.upsert.UpsertQueryCreator import UpsertQueryCreator
from datalakebundle.delta.DeltaStorage import DeltaStorage


class DataWriter:
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        delta_storage: DeltaStorage,
        upsert_query_creator: UpsertQueryCreator,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__delta_storage = delta_storage
        self.__upsert_query_creator = upsert_query_creator

    def append(self, df: DataFrame, full_table_name: str, schema: StructType, options: dict):
        # insertInto() requires dataframe columns order to match schema columns order
        df.select([field.name for field in schema.fields]).write.options(**options).insertInto(full_table_name, overwrite=False)

    def overwrite(self, df: DataFrame, full_table_name: str, partition_by: list, options: dict):
        self.__delta_storage.overwrite_data(df, full_table_name, partition_by, options)

    def upsert(self, df: DataFrame, full_table_name: str, schema: StructType, primary_key: list):
        temp_source_table = (
            f"upsert_{full_table_name.replace('.', '__')}_{''.join(random.choice(string.ascii_lowercase) for _ in range(6))}"
        )

        df.createOrReplaceTempView(temp_source_table)

        upsert_sql_statement = self.__upsert_query_creator.create(full_table_name, schema, primary_key, temp_source_table)

        try:
            self.__spark.sql(upsert_sql_statement)

        except BaseException:
            raise

        finally:
            self.__spark.catalog.dropTempView(temp_source_table)
