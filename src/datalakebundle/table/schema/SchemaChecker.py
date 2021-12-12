import json
from logging import Logger
from datalakebundle.table.schema.DiffGenerator import DiffGenerator
from pyspark.sql.dataframe import DataFrame
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.MetadataChecker import MetadataChecker


class SchemaChecker:
    def __init__(self, logger: Logger, metadata_checker: MetadataChecker, diff_generator: DiffGenerator):
        self.__logger = logger
        self.__metadata_checker = metadata_checker
        self.__diff_generator = diff_generator

    def check_with_metadata(self, df: DataFrame, table_definition: TableDefinition):
        self.check(df, table_definition)
        self.check_metadata(table_definition)

    def check(self, df: DataFrame, table_definition: TableDefinition):
        extra = {"table": table_definition.full_table_name, "diff": self.__diff_generator.generate(df.schema, table_definition.schema)}

        if extra["diff"]:
            error_message = "Table and dataframe schemas do NOT match"
            report = json.dumps(extra, indent=4).replace('"', "")

            self.__logger.error(f"{error_message}\n{report}")
            raise Exception(error_message)

    def check_metadata(self, table_definition: TableDefinition):
        self.__metadata_checker.check(table_definition)
