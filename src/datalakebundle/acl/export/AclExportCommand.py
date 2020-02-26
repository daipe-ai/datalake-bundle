# pylint: disable = invalid-name
from argparse import Namespace, ArgumentParser
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.acl.export.AclExporter import AclExporter

class AclExportCommand(ConsoleCommand):

    def __init__(
        self,
        storages: Box,
        logger: Logger,
        aclExporter: AclExporter,
    ):
        self.__storages = storages
        self.__logger = logger
        self.__aclExporter = aclExporter

    def getCommand(self) -> str:
        return 'gen2datalake:acl:export'

    def getDescription(self):
        return 'Exports ACL settings of given Azure Storage to YAML file'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument('filesystem', help='filesystem name')
        argumentParser.add_argument('path', help='relative path to store data')

    def run(self, inputArgs: Namespace):
        self.__logger.info('Exporting ACL...')

        storage = list(filter(lambda storage: storage['filesystem'] == inputArgs.filesystem, self.__storages))[0]

        self.__aclExporter.export(storage['filesystem'], storage['maxLevel'], inputArgs.path)
