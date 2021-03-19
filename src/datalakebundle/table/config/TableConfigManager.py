from typing import Optional, List
from box import Box
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigParser import TableConfigParser


class TableConfigManager:

    __table_configs: List[TableConfig] = []

    def __init__(
        self,
        raw_table_configs: Box,
        table_defaults: Box,
        table_config_parser: TableConfigParser,
    ):
        raw_table_configs = raw_table_configs or Box(dict())

        self.__table_configs = [
            table_config_parser.parse(identifier, explicit_config or dict(), table_defaults.to_dict())
            for identifier, explicit_config in raw_table_configs.to_dict().items()
        ]

    def exists(self, identifier: str) -> bool:
        for table_config in self.__table_configs:
            if table_config.identifier == identifier:
                return True

        return False

    def get(self, identifier: str) -> Optional[TableConfig]:
        for table_config in self.__table_configs:
            if table_config.identifier == identifier:
                return table_config

        raise Exception(f"Identifier {identifier} not found among datalakebundle.tables")

    def get_all(self):
        return self.__table_configs

    def get_by_filter(self, filter_function: callable):
        return list(filter(filter_function, self.__table_configs))

    def get_with_attribute(self, attr_name: str):
        return self.get_by_filter(lambda table_config: hasattr(table_config, attr_name))
