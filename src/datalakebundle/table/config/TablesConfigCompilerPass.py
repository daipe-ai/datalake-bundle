from box import Box
from injecta.compiler.CompilerPassInterface import CompilerPassInterface
from injecta.container.ContainerBuild import ContainerBuild
from injecta.dtype.classLoader import loadClass
from injecta.service.DTypeResolver import DTypeResolver
from datalakebundle.table.identifier.IdentifierParser import IdentifierParser
from datalakebundle.table.config.TablesConfigParser import TablesConfigParser

class TablesConfigCompilerPass(CompilerPassInterface):

    def __init__(self):
        self.__dTypeResolver = DTypeResolver()
        self.__tablesConfigParser = TablesConfigParser()

    def process(self, containerBuild: ContainerBuild):
        parameters = containerBuild.parameters # type: Box

        if 'datalakebundle' not in parameters:
            return

        bundleParameters = parameters.datalakebundle
        identifierParameters = bundleParameters.table.identifier

        if not identifierParameters.parsingEnabled:
            return

        identifierParserType = self.__dTypeResolver.resolve(identifierParameters.parser['class'])
        identifierParserClass = loadClass(identifierParserType.moduleName, identifierParserType.className)
        identifierParser = identifierParserClass(*identifierParameters.parser.arguments) # type: IdentifierParser

        if bundleParameters.tables:
            bundleParameters.tables = Box(self.__tablesConfigParser.parse(
                bundleParameters.tables.to_dict(),
                identifierParameters.transformations.to_dict() if 'transformations' in identifierParameters else dict(),
                identifierParser,
            ))

        if bundleParameters.externalTables:
            bundleParameters.externalTables = Box(self.__tablesConfigParser.parse(
                bundleParameters.externalTables.to_dict(),
                identifierParameters.transformations.to_dict() if 'transformations' in identifierParameters else dict(),
                identifierParser,
            ))
