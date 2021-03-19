from box import Box
from datalakebundle.table.identifier import placeholders_extractor
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface


class NotebookModuleResolver(ValueResolverInterface):
    def __init__(
        self,
        notebook_module_template: str,
    ):
        self.__notebook_module_template = notebook_module_template

    def resolve(self, raw_table_config: Box):
        return self.__notebook_module_template.format(**raw_table_config)

    def get_depending_fields(self) -> set:
        return placeholders_extractor.extract(self.__notebook_module_template)
