from box import Box
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class TargetPathResolver(ValueResolverInterface):

    def __init__(self, basePath: str):
        self.__basePath = basePath

    def resolve(self, rawTableConfig: Box):
        encryptedString = 'encrypted' if rawTableConfig.encrypted is True else 'plain'

        return self.__basePath + '/' + rawTableConfig.dbIdentifierBase + '/' + encryptedString + '/' + rawTableConfig.tableIdentifier + '.delta'
