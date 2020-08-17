from box import Box
from injecta.compiler.CompilerPassInterface import CompilerPassInterface
from injecta.container.ContainerBuild import ContainerBuild
from injecta.service.DTypeResolver import DTypeResolver
from datalakebundle.table.config.TableConfigParser import TableConfigParser

class TablesConfigCompilerPass(CompilerPassInterface):

    def __init__(self):
        self.__dTypeResolver = DTypeResolver()

    def process(self, containerBuild: ContainerBuild):
        parameters = containerBuild.parameters # type: Box

        if 'datalakebundle' not in parameters:
            return

        bundleParameters = parameters.datalakebundle

        if not bundleParameters.tables:
            return

        tableConfigParser = TableConfigParser(bundleParameters.table.nameTemplate)

        tables = {
            identifier: tableConfigParser.parse(
                identifier,
                explicitConfig or dict(),
                bundleParameters.table.defaults if 'defaults' in bundleParameters.table else dict()
            )
            for identifier, explicitConfig in bundleParameters.tables.to_dict().items()
        }

        bundleParameters.tables = Box(tables)
