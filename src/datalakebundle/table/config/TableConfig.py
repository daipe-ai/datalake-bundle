# pylint: disable = too-many-instance-attributes
import json
import os
from pathlib import Path
from injecta.package.pathResolver import resolvePath
from injecta.module import attributeLoader
import pyspark.sql.types as t

class TableConfig:

    def __init__(
        self,
        identifier: str,
        dbIdentifier: str,
        dbName: str,
        tableIdentifier: str,
        tableName: str,
        schemaLoader: str,
        targetPath: str,
        notebookModule: str,
        partitionBy: list = None,
        **kwargs,
    ):
        self.__identifier = identifier
        self.__dbIdentifier = dbIdentifier
        self.__dbName = dbName
        self.__tableIdentifier = tableIdentifier
        self.__tableName = tableName
        self.__schemaLoader = schemaLoader
        self.__targetPath = targetPath
        self.__notebookModule = notebookModule
        self.__partitionBy = partitionBy or []
        self.__customFields = kwargs

    @property
    def identifier(self):
        return self.__identifier

    @property
    def dbIdentifier(self):
        return self.__dbIdentifier

    @property
    def dbName(self):
        return self.__dbName

    @property
    def tableIdentifier(self):
        return self.__tableIdentifier

    @property
    def tableName(self):
        return self.__tableName

    @property
    def schemaLoader(self):
        return self.__schemaLoader

    @property
    def schema(self) -> t.StructType:
        getSchema = attributeLoader.loadFromString(self.__schemaLoader)

        return getSchema()

    @property
    def targetPath(self):
        return self.__targetPath

    @property
    def notebookModule(self):
        return self.__notebookModule

    @property
    def notebookPath(self) -> Path:
        parts = self.__notebookModule.split('.')
        basePath = resolvePath(parts[0])

        return Path(basePath + os.sep + os.sep.join(parts[1:]) + '.py')

    @property
    def partitionBy(self):
        return self.__partitionBy

    @property
    def fullTableName(self):
        return self.__dbName + '.' + self.__tableName

    def getCustomFields(self):
        return self.__customFields

    def asDict(self):
        return {**self.__customFields, **{
            'identifier': self.__identifier,
            'dbIdentifier': self.__dbIdentifier,
            'dbName': self.__dbName,
            'tableIdentifier': self.__tableIdentifier,
            'tableName': self.__tableName,
            'schemaLoader': self.__schemaLoader,
            'targetPath': self.__targetPath,
            'notebookModule': self.__notebookModule,
            'partitionBy': self.__partitionBy,
        }}

    def __getattr__(self, item):
        if item not in self.__customFields:
            raise Exception(f'Unexpected attribute: "{item}"')

        return self.__customFields[item]

    def __eq__(self, other: 'TableConfig'):
        return self.asDict() == other.asDict()

    def __repr__(self):
        return json.dumps(self.asDict(), indent=4)
