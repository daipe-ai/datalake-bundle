import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.class_ import table_schema_loader
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.create.TableDefinitionFactory import TableDefinitionFactory


class TableCreatorCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_definition_factory: TableDefinitionFactory,
        table_existence_checker: TableExistenceChecker,
        table_creator: TableCreator,
    ):
        self.__logger = logger
        self.__table_definition_factory = table_definition_factory
        self.__table_existence_checker = table_existence_checker
        self.__table_creator = table_creator

    def get_command(self) -> str:
        return "datalake:table:create"

    def get_description(self):
        return "Creates a metastore table based on it's identifier and table class"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")
        argument_parser.add_argument(dest="table_schema_path", help="Table class path [module_path].[class_name]")

    def run(self, input_args: Namespace):
        table_definition = self.__table_definition_factory.create_from_table_schema(
            input_args.identifier, table_schema_loader.load(input_args.table_schema_path)
        )

        if self.__table_existence_checker.table_exists(table_definition.db_name, table_definition.table_name):
            self.__logger.error(f"Hive table {table_definition.full_table_name} already exists")
            sys.exit(1)

        self.__table_creator.create(table_definition)
