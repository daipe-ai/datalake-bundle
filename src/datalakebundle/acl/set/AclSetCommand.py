# pylint: disable = invalid-name
from argparse import Namespace, ArgumentParser
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from datalakebundle.acl.set.AclSetter import AclSetter

class AclSetCommand(ConsoleCommand):

    def __init__(
        self,
        storages: Box,
        logger: Logger,
        aclSetter: AclSetter,
    ):
        self.__storages = storages
        self.__logger = logger
        self.__aclSetter = aclSetter

    def getCommand(self) -> str:
        return 'gen2datalake:acl:set'

    def getDescription(self):
        return 'Sets ACL permissions defined in YAML config to given GEN2 DataLake'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument('filesystem', help='filesystem name')
        argumentParser.add_argument('path', help='relative path to stored YAML (not a filename)')

    def run(self, inputArgs: Namespace):
        self.__logger.info('Setting ACL to filesystem...')

        storage = list(filter(lambda storage: storage['filesystem'] == inputArgs.filesystem, self.__storages))[0]

        self.__aclSetter.set(storage['filesystem'], inputArgs.path)
