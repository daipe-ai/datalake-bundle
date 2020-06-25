from box import Box
from datalakebundle.table.identifier.DefaultValueResolverInterface import DefaultValueResolverInterface

class TargetPathResolver(DefaultValueResolverInterface):

    def __init__(self, basePath: str):
        self.__basePath = basePath

    def resolve(self, rawTableConfig: Box):
        encryptedString = 'encrypted' if rawTableConfig.encrypted is True else 'plain'

        return self.__basePath + '/' + rawTableConfig.dbNameWithoutEnv + '/' + encryptedString + '/' + rawTableConfig.dbNameWithoutEnv + '.delta'
