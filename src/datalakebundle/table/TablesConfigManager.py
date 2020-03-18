from typing import Optional
from box import Box
from datalakebundle.table.TableConfig import TableConfig

class TablesConfigManager:

    def __init__(
        self,
        tableConfigs: Box,
    ):
        self.__tableConfigs = list(map(lambda tc: TableConfig.fromBox(tc[0], tc[1]), tableConfigs.items()))

    def getByAlias(self, configAlias: str) -> Optional[TableConfig]:
        for tableConfig in self.__tableConfigs:
            if tableConfig.alias == configAlias:
                return tableConfig

        return None

    def getByFilter(self, filterFunction: callable):
        return list(filter(filterFunction, self.__tableConfigs))
