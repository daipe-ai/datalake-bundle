# pylint: disable = protected-access
import sys
from argparse import Namespace

def tableActionCommand(originalClass):
    originalRun = originalClass.run

    def run(self, inputArgs: Namespace):
        if not self._tableConfigManager.exists(inputArgs.identifier):
            self._logger.error(f'Identifier {inputArgs.identifier} not found among datalakebundle.tables')
            sys.exit(1)

        originalRun(self, inputArgs)

    originalClass.run = run
    return originalClass
