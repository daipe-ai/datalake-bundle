# pylint: disable = invalid-name
from argparse import Namespace, ArgumentParser
from logging import Logger
from box import Box
from datalakebundle.acl.check.AclChecker import AclChecker
from consolebundle.ConsoleCommand import ConsoleCommand

class AclCheckCommand(ConsoleCommand):

    def __init__(
        self,
        storages: Box,
        logger: Logger,
        aclChecker: AclChecker,
    ):
        self.__storages = storages
        self.__logger = logger
        self.__aclChecker = aclChecker

    def getCommand(self) -> str:
        return 'gen2datalake:acl:check'

    def getDescription(self):
        return 'Checks setting of Filesystem and Folders and compares it against definition in YAML.'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument('filesystem', help='filesystem name')
        argumentParser.add_argument('path', help='relative path to stored YAML (not a filename)')

    def run(self, inputArgs: Namespace):
        self.__logger.info('Checking ACL in filesystem')

        storage = list(filter(lambda storage: storage['filesystem'] == inputArgs.filesystem, self.__storages))[0]

        self.__aclChecker.check(storage['filesystem'], inputArgs.path)

        self.__logger.info('ACL check done')
