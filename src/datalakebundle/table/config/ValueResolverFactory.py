from injecta.dtype.classLoader import loadClass
from injecta.service.parser.DTypeResolver import DTypeResolver
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class ValueResolverFactory:

    def __init__(self):
        self.__dTypeResolver = DTypeResolver()

    def create(self, val) -> ValueResolverInterface:
        resolverType = self.__dTypeResolver.resolve(val['resolverClass'])
        resolverClass = loadClass(resolverType.moduleName, resolverType.className)
        resolverArguments = val['resolverArguments'] if 'resolverArguments' in val else []

        return resolverClass(*resolverArguments)
