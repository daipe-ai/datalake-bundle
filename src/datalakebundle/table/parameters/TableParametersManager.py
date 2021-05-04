from typing import List
from box import Box
from datalakebundle.table.parameters.TableParameters import TableParameters
from datalakebundle.table.parameters.TableParametersParser import TableParametersParser


class TableParametersManager:

    __table_parameters: List[TableParameters] = []

    def __init__(
        self,
        raw_table_parameters: Box,
        table_defaults: Box,
        table_parameters_parser: TableParametersParser,
    ):
        self.__table_defaults = table_defaults.to_dict() if table_defaults else dict()
        self.__table_parameters_parser = table_parameters_parser

        raw_table_parameters = raw_table_parameters or Box(dict())

        self.__table_parameters = [
            table_parameters_parser.parse(identifier, self.__table_defaults, explicit_parameters or dict())
            for identifier, explicit_parameters in raw_table_parameters.to_dict().items()
        ]

    def exists(self, identifier: str) -> bool:
        for table_parameters in self.__table_parameters:
            if table_parameters.identifier == identifier:
                return True

        return False

    def get_or_parse(self, identifier: str) -> TableParameters:
        for table_parameters in self.__table_parameters:
            if table_parameters.identifier == identifier:
                return table_parameters

        # no explicitly defined table parameters found, parse the basic parameters from identifier only
        return self.__table_parameters_parser.parse(identifier, self.__table_defaults)

    def get_all(self):
        return self.__table_parameters

    def get_by_filter(self, filter_function: callable):
        return list(filter(filter_function, self.__table_parameters))

    def get_with_attribute(self, attr_name: str):
        return self.get_by_filter(lambda table_parameters: hasattr(table_parameters, attr_name))
