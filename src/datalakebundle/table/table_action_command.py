import sys
from argparse import Namespace


def table_action_command(original_class):
    original_run = original_class.run

    def run(self, input_args: Namespace):
        if not self._table_config_manager.exists(input_args.identifier):
            self._logger.error(f"Identifier {input_args.identifier} not found among datalakebundle.tables")
            sys.exit(1)

        original_run(self, input_args)

    original_class.run = run
    return original_class
