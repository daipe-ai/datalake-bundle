import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.UnknownTableException import UnknownTableException
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.delete.TableDeleter import TableDeleter
from datalakebundle.table.table_action_command import table_action_command


@table_action_command
class TableDeleterCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_config_manager: TableConfigManager,
        table_deleter: TableDeleter,
    ):
        self._logger = logger
        self._table_config_manager = table_config_manager
        self._table_deleter = table_deleter

    def get_command(self) -> str:
        return "datalake:table:delete"

    def get_description(self):
        return "Deletes a metastore table including data on HDFS"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")

    def run(self, input_args: Namespace):
        table_config = self._table_config_manager.get(input_args.identifier)

        try:
            self._table_deleter.delete(table_config)

            self._logger.info(f"Table {table_config.full_table_name} successfully deleted")
        except UnknownTableException as e:
            self._logger.error(str(e))
            sys.exit(1)
