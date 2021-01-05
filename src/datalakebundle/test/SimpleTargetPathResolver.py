from box import Box
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class SimpleTargetPathResolver(ValueResolverInterface):

    def __init__(self, basePath: str):
        self.__basePath = basePath

    def resolve(self, rawTableConfig: Box):
        return self.__basePath + '/' + rawTableConfig.dbIdentifier + '/' + rawTableConfig.tableIdentifier + '.delta'

    def getDependingFields(self) -> set:
        return {'dbIdentifier', 'tableIdentifier'}
