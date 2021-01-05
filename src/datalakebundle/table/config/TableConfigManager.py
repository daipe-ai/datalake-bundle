from typing import Optional, List
from box import Box
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigParser import TableConfigParser

class TableConfigManager:

    __tableConfigs: List[TableConfig] = []

    def __init__(
        self,
        rawTableConfigs: Box,
        tableDefaults: Box,
        tableConfigParser: TableConfigParser,
    ):
        rawTableConfigs = rawTableConfigs or Box(dict())

        self.__tableConfigs = [
            tableConfigParser.parse(
                identifier,
                explicitConfig or dict(),
                tableDefaults.to_dict()
            )
            for identifier, explicitConfig in rawTableConfigs.to_dict().items()
        ]

    def exists(self, identifier: str) -> bool:
        for tableConfig in self.__tableConfigs:
            if tableConfig.identifier == identifier:
                return True

        return False

    def get(self, identifier: str) -> Optional[TableConfig]:
        for tableConfig in self.__tableConfigs:
            if tableConfig.identifier == identifier:
                return tableConfig

        raise Exception(f'Identifier {identifier} not found among datalakebundle.tables')

    def getAll(self):
        return self.__tableConfigs

    def getByFilter(self, filterFunction: callable):
        return list(filter(filterFunction, self.__tableConfigs))

    def getWithAttribute(self, attrName: str):
        return self.getByFilter(lambda tableConfig: hasattr(tableConfig, attrName))
