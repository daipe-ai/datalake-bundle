from box import Box

class ExternalTableConfig:

    def __init__(
        self,
        identifier: str,
        dbName: str,
        tableName: str,
    ):
        self.__identifier = identifier
        self.__dbName = dbName
        self.__tableName = tableName

    @property
    def identifier(self):
        return self.__identifier

    @property
    def tableName(self):
        return self.__tableName

    @property
    def fullTableName(self):
        return self.__dbName + '.' + self.__tableName

    @property
    def dbName(self):
        return self.__dbName

    @staticmethod
    def fromBox(identifier: str, boxConfig: Box) -> 'ExternalTableConfig':
        return ExternalTableConfig(
            identifier=identifier,
            dbName=boxConfig.dbName,
            tableName=boxConfig.tableName,
        )
