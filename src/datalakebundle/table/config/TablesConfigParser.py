from box import Box
from injecta.dtype.classLoader import loadClass
from injecta.service.DTypeResolver import DTypeResolver
from datalakebundle.table.identifier.IdentifierParserInterface import IdentifierParserInterface
from datalakebundle.table.identifier.DefaultValueResolverInterface import DefaultValueResolverInterface
from datalakebundle.table.identifier.Expression import Expression
from datalakebundle.table.identifier.fillTemplate import fillTemplate

class TablesConfigParser:

    def __init__(self):
        self.__dTypeResolver = DTypeResolver()

    def parse(self, tablesConfig: dict, transformations: dict, identifierParser: IdentifierParserInterface):
        tables = dict()
        defaults = tablesConfig['_defaults'] if '_defaults' in tablesConfig else dict()
        tablesConfig = {identifier: table for identifier, table in tablesConfig.items() if identifier != '_defaults'}

        for identifier, explicitConfig in tablesConfig.items():
            explicitConfig = explicitConfig or dict()
            parsedAttributes = identifierParser.parse(identifier)

            for name, transformation in transformations.items():
                parsedAttributes[name] = self.__transformAttribute(parsedAttributes, transformation)

            baseConfig = {**parsedAttributes, **explicitConfig}
            processedDefaults = {name: self.__processDefault(val, baseConfig) for name, val in defaults.items()}

            tables[identifier] = {**processedDefaults, **baseConfig}

        return tables

    def __transformAttribute(self, parsedAttributes: dict, transformation: Expression):
        if not isinstance(transformation, Expression):
            raise Exception('Transformation must be an expression (!expr \'python expression\')')

        return transformation.evaluate(parsedAttributes)

    def __processDefault(self, val, rawTableConfig: dict):
        if isinstance(val, Expression):
            return val.evaluate(rawTableConfig)

        if isinstance(val, dict):
            resolverType = self.__dTypeResolver.resolve(val['resolverClass'])
            resolverClass = loadClass(resolverType.moduleName, resolverType.className)
            resolver = resolverClass(*val['resolverArguments']) # type: DefaultValueResolverInterface

            return resolver.resolve(Box(rawTableConfig))

        if isinstance(val, str):
            return fillTemplate(val, rawTableConfig)

        return val
