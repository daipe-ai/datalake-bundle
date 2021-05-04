from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pysparkbundle.filesystem.FilesystemInterface import FilesystemInterface
from datalakebundle.table.create.TableDefinition import TableDefinition


class DeltaStorage:
    def __init__(
        self,
        filesystem: FilesystemInterface,
        spark: SparkSession,
    ):
        self.__filesystem = filesystem
        self.__spark = spark

    def create_table(self, table_definition: TableDefinition, options: dict = None):
        df = self.__create_empty_df(table_definition.schema)

        self.__create(df, table_definition, "errorifexists", options or {})

    def recreate_table(self, table_definition: TableDefinition, options: dict = None):
        self.__spark.sql(f"DROP TABLE IF EXISTS {table_definition.full_table_name}")
        self.__filesystem.delete(table_definition.target_path, True)
        self.create_table(table_definition, options or {})

    def overwrite_data(self, df: DataFrame, full_table_name: str, partition_by: list, options: dict):
        options = options or {}
        base_options = {"overwriteSchema": "true"}
        merged_options = {**options, **base_options}

        self.__save_as_table(df, full_table_name, "overwrite", partition_by, merged_options)

    def __create(self, df: DataFrame, table_definition: TableDefinition, mode: str, options: dict):
        base_options = {"path": table_definition.target_path}
        merged_options = {**options, **base_options}

        self.__save_as_table(df, table_definition.full_table_name, mode, table_definition.partition_by, merged_options)

    def __save_as_table(self, df: DataFrame, full_table_name: str, mode: str, partition_by: list, options: dict):
        (df.write.partitionBy(partition_by).format("delta").options(**options).mode(mode).saveAsTable(full_table_name))

    def __create_empty_df(self, schema: StructType):
        return self.__spark.createDataFrame([], schema)
