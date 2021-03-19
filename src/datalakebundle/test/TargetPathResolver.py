from box import Box
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface


class TargetPathResolver(ValueResolverInterface):
    def __init__(self, base_path: str):
        self.__base_path = base_path

    def resolve(self, raw_table_config: Box):
        encrypted_string = "encrypted" if raw_table_config.encrypted is True else "plain"

        return (
            self.__base_path
            + "/"
            + raw_table_config.db_identifier_base
            + "/"
            + encrypted_string
            + "/"
            + raw_table_config.table_identifier
            + ".delta"
        )
