from datalakebundle.table.config.ValueResolver import ValueResolver
from datalakebundle.table.identifier.IdentifierParser import IdentifierParser
from datalakebundle.table.identifier.fillTemplate import fillTemplate

class TableConfigParser:

    def __init__(
        self,
        tableNameTemplate: str
    ):
        self.__valueResolver = ValueResolver()
        self.__identifierParser = IdentifierParser()
        self.__tableNameTemplate = tableNameTemplate

    def parse(self, identifier: str, explicitConfig: dict, defaultConfig: dict = None):
        defaultConfig = defaultConfig or dict()
        identifiers = self.__identifierParser.parse(identifier)
        tableNameParts = self.__resolveTableNameParts(identifiers)

        allFields = {**identifiers, **tableNameParts, **explicitConfig}

        for name, resolver in defaultConfig.items():
            if name not in allFields:
                allFields[name] = self.__valueResolver.resolve(resolver, allFields)

        return allFields

    def __resolveTableNameParts(self, identifiers: dict):
        fullTableName = fillTemplate(self.__tableNameTemplate, identifiers)
        dotPosition = fullTableName.find('.')

        if dotPosition == -1:
            raise Exception('Table name must meet the following format: {dbName}.{tableName}')

        return {
            'dbName': fullTableName[:dotPosition],
            'tableName': fullTableName[dotPosition + 1:],
        }
