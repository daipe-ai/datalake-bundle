from box import Box
from injecta.dtype.classLoader import loadClass
from injecta.service.DTypeResolver import DTypeResolver
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface
from datalakebundle.table.identifier.Expression import Expression
from datalakebundle.table.identifier.fillTemplate import fillTemplate

class ValueResolver:

    def __init__(self):
        self.__dTypeResolver = DTypeResolver()

    def resolve(self, val, rawTableConfig: dict):
        if isinstance(val, Expression):
            return val.evaluate(rawTableConfig)

        if isinstance(val, dict):
            resolverType = self.__dTypeResolver.resolve(val['resolverClass'])
            resolverClass = loadClass(resolverType.moduleName, resolverType.className)
            resolver = resolverClass(*val['resolverArguments']) # type: ValueResolverInterface

            return resolver.resolve(Box(rawTableConfig))

        if isinstance(val, str):
            return fillTemplate(val, rawTableConfig)

        return val
