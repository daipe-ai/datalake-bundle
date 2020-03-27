from box import Box

class ExternalTableConfig:

    def __init__(
        self,
        alias: str,
        tableName: str,
    ):
        self.__alias = alias
        self.__tableName = tableName

    @property
    def alias(self):
        return self.__alias

    @property
    def tableName(self):
        return self.__tableName[self.__tableName.find('.') + 1:]

    @property
    def fullTableName(self):
        return self.__tableName

    @property
    def dbName(self):
        return self.__tableName[0:self.__tableName.find('.')]

    @staticmethod
    def fromBox(configAlias: str, boxConfig: Box) -> 'ExternalTableConfig':
        return ExternalTableConfig(
            alias=configAlias,
            tableName=boxConfig.tableName,
        )
