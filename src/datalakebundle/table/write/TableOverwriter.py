from logging import Logger
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.SchemaChecker import SchemaChecker
from datalakebundle.table.write.DataWriter import DataWriter
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.create.TableRecreator import TableRecreator


class TableOverwriter:
    def __init__(
        self,
        logger: Logger,
        schema_checker: SchemaChecker,
        table_creator: TableCreator,
        table_recreator: TableRecreator,
        data_writer: DataWriter,
    ):
        self.__logger = logger
        self.__schema_checker = schema_checker
        self.__table_creator = table_creator
        self.__table_recreator = table_recreator
        self.__data_writer = data_writer

    def overwrite(self, result: DataFrame, table_definition: TableDefinition, recreate_table: bool, options: dict):
        output_table_name = table_definition.full_table_name

        self.__schema_checker.check(result, output_table_name, table_definition.schema)

        self.__logger.info(f"Data to be written into table: {output_table_name}")

        if recreate_table:
            self.__table_recreator.recreate(table_definition)
        else:
            self.__table_creator.create_if_not_exists(table_definition)

        self.__logger.info(f"Writing data to table: {output_table_name}")

        self.__data_writer.overwrite(result, output_table_name, table_definition.partition_by, options)

        self.__logger.info(f"Data successfully written to: {output_table_name}")
