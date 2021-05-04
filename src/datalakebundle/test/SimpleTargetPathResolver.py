from box import Box
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface


class SimpleTargetPathResolver(ValueResolverInterface):
    def __init__(self, base_path: str):
        self.__base_path = base_path

    def resolve(self, raw_table_parameters: Box):
        return self.__base_path + "/" + raw_table_parameters.db_identifier + "/" + raw_table_parameters.table_identifier + ".delta"

    def get_depending_fields(self) -> set:
        return {"db_identifier", "table_identifier"}
