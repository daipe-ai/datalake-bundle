from box import Box
from datalakebundle.table.identifier import placeholdersExtractor
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class SchemaLoaderResolver(ValueResolverInterface):

    def __init__(
        self,
        schemaLoaderTemplate: str,
    ):
        self.__schemaLoaderTemplate = schemaLoaderTemplate

    def resolve(self, rawTableConfig: Box):
        rawTableConfig['notebookParentModule'] = self._getParentModule(rawTableConfig['notebookModule'])

        return self.__schemaLoaderTemplate.format(**rawTableConfig)

    def getDependingFields(self) -> set:
        fields = placeholdersExtractor.extract(self.__schemaLoaderTemplate)
        fields.add('notebookModule')

        if 'notebookParentModule' in fields:
            fields.remove('notebookParentModule')

        return fields

    def _getParentModule(self, notebookModule: str):
        return notebookModule[:notebookModule.rfind('.')]
