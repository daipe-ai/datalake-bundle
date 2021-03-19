from argparse import ArgumentParser, Namespace
from logging import Logger
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.table.config.TableConfigManager import TableConfigManager
from datalakebundle.table.create.TableCreator import TableCreator
from datalakebundle.table.table_action_command import table_action_command


@table_action_command
class TableCreatorCommand(ConsoleCommand):
    def __init__(
        self,
        logger: Logger,
        table_config_manager: TableConfigManager,
        table_creator: TableCreator,
    ):
        self._logger = logger
        self._table_config_manager = table_config_manager
        self._table_creator = table_creator

    def get_command(self) -> str:
        return "datalake:table:create"

    def get_description(self):
        return "Creates a metastore table based on it's YAML definition (name, schema, data path, ...)"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Table identifier")

    def run(self, input_args: Namespace):
        table_config = self._table_config_manager.get(input_args.identifier)

        self._logger.info(f"Creating table {table_config.fullTableName} for {table_config.targetPath}")

        self._table_creator.create_empty_table(table_config)

        self._logger.info(f"Table {table_config.fullTableName} successfully created")
