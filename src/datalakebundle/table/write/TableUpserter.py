from logging import Logger
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.SchemaChecker import SchemaChecker
from datalakebundle.table.write.DataWriter import DataWriter
from datalakebundle.table.create.TableCreator import TableCreator


class TableUpserter:
    def __init__(self, logger: Logger, schema_checker: SchemaChecker, table_creator: TableCreator, data_writer: DataWriter):
        self.__logger = logger
        self.__schema_checker = schema_checker
        self.__table_creator = table_creator
        self.__data_writer = data_writer

    def upsert(self, result: DataFrame, table_definition: TableDefinition):
        output_table_name = table_definition.full_table_name

        self.__schema_checker.check_with_metadata(result, table_definition)

        self.__log_upserted_columns(table_definition)

        self.__table_creator.create_if_not_exists(table_definition)

        self.__logger.info(f"Upserting data to table: {output_table_name}")

        self.__data_writer.upsert(result, output_table_name, table_definition.schema, table_definition.primary_key)

        self.__logger.info(f"Data successfully upserted to: {output_table_name}")

    def __log_upserted_columns(self, table_definition: TableDefinition):
        if len(table_definition.primary_key) > 1:
            columns_string = f"columns [{', '.join(table_definition.primary_key)}]"
        else:
            columns_string = f"column {table_definition.primary_key[0]}"

        self.__logger.info(f"Data to be upserted into table: {table_definition.full_table_name} (by {columns_string})")
