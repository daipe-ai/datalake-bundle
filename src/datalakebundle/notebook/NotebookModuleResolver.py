from box import Box
from datalakebundle.table.identifier import placeholdersExtractor
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class NotebookModuleResolver(ValueResolverInterface):

    def __init__(
        self,
        notebookModuleTemplate: str,
    ):
        self.__notebookModuleTemplate = notebookModuleTemplate

    def resolve(self, rawTableConfig: Box):
        return self.__notebookModuleTemplate.format(**rawTableConfig)

    def getDependingFields(self) -> set:
        return placeholdersExtractor.extract(self.__notebookModuleTemplate)
