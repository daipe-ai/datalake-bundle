from datalakebundle.table.config.TablesConfigManager import TablesConfigManager
from datalakebundle.table.identifier.UnknownIdentifierException import UnknownIdentifierException

class TableNames:

    def __init__(
        self,
        tablesConfigManager: TablesConfigManager,
    ):
        self.__tablesConfigManager = tablesConfigManager

    def get(self, identifier: str) -> str:
        tableConfig = self.__tablesConfigManager.get(identifier)

        if tableConfig:
            return tableConfig.fullTableName

        externalTableConfig = self.__tablesConfigManager.getExternal(identifier)

        if externalTableConfig:
            return externalTableConfig.fullTableName

        raise UnknownIdentifierException(identifier)

    # @deprecated
    def getByAlias(self, identifier: str) -> str:
        return self.get(identifier)
