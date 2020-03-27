from typing import Optional
from box import Box
from datalakebundle.table.ExternalTableConfig import ExternalTableConfig
from datalakebundle.table.TableConfig import TableConfig

class TablesConfigManager:

    def __init__(
        self,
        tableConfigs: Box,
        externalTableConfigs: Box,
    ):
        if tableConfigs:
            self.__tableConfigs = list(map(lambda tc: TableConfig.fromBox(tc[0], tc[1]), tableConfigs.items()))
        else:
            self.__tableConfigs = []

        if externalTableConfigs:
            self.__externalTableConfigs = list(map(lambda tc: ExternalTableConfig.fromBox(tc[0], tc[1]), externalTableConfigs.items()))
        else:
            self.__externalTableConfigs = []

    def getByAlias(self, configAlias: str) -> Optional[TableConfig]:
        for tableConfig in self.__tableConfigs:
            if tableConfig.alias == configAlias:
                return tableConfig

        return None

    def getExternalByAlias(self, configAlias: str) -> Optional[ExternalTableConfig]:
        for externalTableConfig in self.__externalTableConfigs:
            if externalTableConfig.alias == configAlias:
                return externalTableConfig

        return None

    def getByFilter(self, filterFunction: callable):
        return list(filter(filterFunction, self.__tableConfigs))

    def getExternalByFilter(self, filterFunction: callable):
        return list(filter(filterFunction, self.__externalTableConfigs))
