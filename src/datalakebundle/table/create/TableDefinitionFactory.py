from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from datalakebundle.table.parameters.TableParameters import TableParameters
from datalakebundle.table.parameters.TableParametersManager import TableParametersManager
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.TableSchemaGenerator import TableSchemaGenerator
from datalakebundle.table.schema.TableSchema import TableSchema


class TableDefinitionFactory:
    def __init__(self, logger: Logger, table_parameters_manager: TableParametersManager, table_schema_generator: TableSchemaGenerator):
        self.__logger = logger
        self.__table_parameters_manager = table_parameters_manager
        self.__table_schema_generator = table_schema_generator

    def create_from_table_schema(self, identifier: str, table_schema: TableSchema):
        table_parameters = self.__table_parameters_manager.get_or_parse(identifier)

        schema = StructType(table_schema.fields)
        primary_key = [table_schema.primary_key] if isinstance(table_schema.primary_key, str) else table_schema.primary_key

        if hasattr(table_schema, "partition_by"):
            partition_by = [table_schema.partition_by] if isinstance(table_schema.partition_by, str) else table_schema.partition_by
        else:
            partition_by = []

        tbl_properties = table_schema.tbl_properties

        return self.__create(table_parameters, schema, primary_key, partition_by, tbl_properties)

    def create_from_dataframe(self, identifier: str, df: DataFrame, decorator_name: str):
        self.__logger.warning(
            f"No explicit schema provided, using dataframe schema instead. "
            f"You can define schema as:\n\n{self.__table_schema_generator.generate(df.schema)}\n"
            f'Usage: @{decorator_name}("{identifier}", get_schema())'
        )
        table_parameters = self.__table_parameters_manager.get_or_parse(identifier)

        return self.__create(table_parameters, df.schema, [], [], {})

    def __create(self, table_parameters: TableParameters, schema: StructType, primary_key: list, partition_by: list, tbl_properties: dict):
        return TableDefinition(
            table_parameters.db_name,
            table_parameters.table_name,
            schema,
            primary_key,
            partition_by,
            table_parameters.target_path,
            tbl_properties,
        )
