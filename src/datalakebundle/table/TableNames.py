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

        if not tableConfig:
            raise UnknownConfigAliasException(configAlias)

        return tableConfig.fullTableName
