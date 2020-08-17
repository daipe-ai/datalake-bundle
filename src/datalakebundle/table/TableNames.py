from datalakebundle.table.identifier.IdentifierParser import IdentifierParser
from datalakebundle.table.identifier.fillTemplate import fillTemplate

class TableNames:

    def __init__(
        self,
        tableNameTemplate: str,
        identifierParser: IdentifierParser
    ):
        self.__tableNameTemplate = tableNameTemplate
        self.__identifierParser = identifierParser

    def get(self, identifier: str) -> str:
        return fillTemplate(self.__tableNameTemplate, self.__identifierParser.parse(identifier))

    # @deprecated
    def getByAlias(self, identifier: str) -> str:
        return self.get(identifier)
