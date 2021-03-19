from box import Box
from datalakebundle.table.identifier import placeholders_extractor
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface


class SchemaLoaderResolver(ValueResolverInterface):
    def __init__(
        self,
        schema_loader_template: str,
    ):
        self.__schema_loader_template = schema_loader_template

    def resolve(self, raw_table_config: Box):
        raw_table_config["notebook_parent_module"] = self._get_parent_module(raw_table_config["notebook_module"])

        return self.__schema_loader_template.format(**raw_table_config)

    def get_depending_fields(self) -> set:
        fields = placeholders_extractor.extract(self.__schema_loader_template)
        fields.add("notebook_module")

        if "notebook_parent_module" in fields:
            fields.remove("notebook_parent_module")

        return fields

    def _get_parent_module(self, notebook_module: str):
        return notebook_module[: notebook_module.rfind(".")]
