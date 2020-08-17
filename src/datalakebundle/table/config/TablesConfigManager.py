from typing import Optional
from box import Box
from datalakebundle.table.config.TableConfig import TableConfig

class TablesConfigManager:

    def __init__(
        self,
        tableConfigs: Box,
    ):
        if tableConfigs:
            self.__tableConfigs = list(map(lambda tc: TableConfig.fromBox(tc[0], tc[1]), tableConfigs.items()))
        else:
            self.__tableConfigs = []

    def get(self, identifier: str) -> Optional[TableConfig]:
        for tableConfig in self.__tableConfigs:
            if tableConfig.identifier == identifier:
                return tableConfig

        return None

    def getByFilter(self, filterFunction: callable):
        return list(filter(filterFunction, self.__tableConfigs))
