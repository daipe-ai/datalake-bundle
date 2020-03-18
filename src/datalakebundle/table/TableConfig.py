from box import Box

class TableConfig:

    def __init__(
        self,
        alias: str,
        tableName: str,
        schemaPath: str,
        targetPath: str,
        partitionBy: list
    ):
        self.__alias = alias
        self.__tableName = tableName
        self.__schemaPath = schemaPath
        self.__targetPath = targetPath
        self.__partitionBy = partitionBy

    @property
    def alias(self):
        return self.__alias

    @property
    def tableName(self):
        return self.__tableName[self.__tableName.find('.') + 1:]

    @property
    def schemaPath(self):
        return self.__schemaPath

    @property
    def targetPath(self):
        return self.__targetPath

    @property
    def partitionBy(self):
        return self.__partitionBy

    @property
    def fullTableName(self):
        return self.__tableName

    @property
    def dbName(self):
        return self.__tableName[0:self.__tableName.find('.')]

    @staticmethod
    def fromBox(configAlias: str, boxConfig: Box) -> 'TableConfig':
        if 'partitionBy' in boxConfig:
            if isinstance(boxConfig.partitionBy, str):
                partitionBy = [boxConfig.partitionBy]
            else:
                partitionBy = boxConfig.partitionBy
        else:
            partitionBy = []

        return TableConfig(
            alias=configAlias,
            tableName=boxConfig.tableName,
            schemaPath=boxConfig.schemaPath,
            targetPath=boxConfig.targetPath,
            partitionBy=partitionBy
        )
