import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from time import sleep
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.UnknownTableException import UnknownTableException
from datalakebundle.table.parameters.TableParametersManager import TableParametersManager
from datalakebundle.table.delete.TableDeleter import TableDeleter


class TableDeleterCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_parameters_manager: TableParametersManager,
        table_existence_checker: TableExistenceChecker,
        table_deleter: TableDeleter,
    ):
        self.__logger = logger
        self.__table_parameters_manager = table_parameters_manager
        self.__table_existence_checker = table_existence_checker
        self.__table_deleter = table_deleter

    def get_command(self) -> str:
        return "datalake:table:delete-including-data"

    def get_description(self):
        return "Deletes a Hive table including all data"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")
        argument_parser.add_argument(
            "-s",
            "--skip-countdown",
            action="store_true",
            help="Skip data deletion countdown",
        )

    def run(self, input_args: Namespace):
        table_parameters = self.__table_parameters_manager.get_or_parse(input_args.identifier)

        if not self.__table_existence_checker.table_exists(table_parameters.db_name, table_parameters.table_name):
            self.__logger.error(f"Hive table {table_parameters.full_table_name} does NOT exists")
            sys.exit(1)

        if input_args.skip_countdown is False:
            self.__logger.info("Use the --skip-countdown switch to delete existing data immediately")

            countdown = 10

            for i in range(countdown):
                self.__logger.warning(f"Table data will be deleted in {countdown - i}s. Use CTRL+C to cancel.")
                sleep(1)

        try:
            self.__table_deleter.delete_including_data(table_parameters)
        except UnknownTableException as e:
            self.__logger.error(str(e))
            sys.exit(1)
