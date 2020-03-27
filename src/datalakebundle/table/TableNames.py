from datalakebundle.table.TablesConfigManager import TablesConfigManager
from datalakebundle.table.UnknownConfigAliasException import UnknownConfigAliasException

class TableNames:

    def __init__(
        self,
        tablesConfigManager: TablesConfigManager,
    ):
        self.__tablesConfigManager = tablesConfigManager

    def getByAlias(self, configAlias: str) -> str:
        tableConfig = self.__tablesConfigManager.getByAlias(configAlias)

        if tableConfig:
            return tableConfig.fullTableName

        externalTableConfig = self.__tablesConfigManager.getExternalByAlias(configAlias)

        if externalTableConfig:
            return externalTableConfig.fullTableName

        raise UnknownConfigAliasException(configAlias)
