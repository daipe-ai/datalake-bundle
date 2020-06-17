from box import Box

class ExternalTableConfig:

    def __init__(
        self,
        identifier: str,
        tableName: str,
    ):
        self.__identifier = identifier
        self.__tableName = tableName

    @property
    def identifier(self):
        return self.__identifier

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
    def fromBox(identifier: str, boxConfig: Box) -> 'ExternalTableConfig':
        return ExternalTableConfig(
            identifier=identifier,
            tableName=boxConfig.tableName,
        )
