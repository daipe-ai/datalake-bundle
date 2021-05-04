from logging import Logger
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.SchemaChecker import SchemaChecker
from datalakebundle.table.write.DataWriter import DataWriter
from datalakebundle.table.create.TableCreator import TableCreator


class TableAppender:
    def __init__(self, logger: Logger, schema_checker: SchemaChecker, table_creator: TableCreator, data_writer: DataWriter):
        self.__logger = logger
        self.__schema_checker = schema_checker
        self.__table_creator = table_creator
        self.__data_writer = data_writer

    def append(self, result: DataFrame, table_definition: TableDefinition, options: dict):
        output_table_name = table_definition.full_table_name

        self.__schema_checker.check(result, output_table_name, table_definition.schema)

        self.__logger.info(f"Data to be appended into table: {output_table_name}")

        self.__table_creator.create_if_not_exists(table_definition)

        self.__logger.info(f"Appending data to table: {output_table_name}")

        self.__data_writer.append(result, output_table_name, table_definition.schema, options)

        self.__logger.info(f"Data successfully appended to: {output_table_name}")
