from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.create.TableRecreator import TableRecreator
from datalakebundle.table.table_action_command import table_action_command


@table_action_command
class TableRecreatorCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_config_manager: TableConfigManager,
        table_recreator: TableRecreator,
    ):
        self._logger = logger
        self._table_config_manager = table_config_manager
        self._table_recreator = table_recreator

    def get_command(self) -> str:
        return "datalake:table:recreate"

    def get_description(self):
        return "Recreates a metastore table based on it's YAML definition (name, schema, data path, ...)"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")

    def run(self, input_args: Namespace):
        table_config = self._table_config_manager.get(input_args.identifier)

        self._table_recreator.recreate(table_config)
