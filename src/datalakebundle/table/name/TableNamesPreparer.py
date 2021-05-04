from datalakebundle.table.identifier.IdentifierParser import IdentifierParser
from datalakebundle.table.name.TableNamesParser import TableNamesParser


class TableNamesPreparer:
    def __init__(self, identifier_parser: IdentifierParser, table_names_parser: TableNamesParser):
        self.__identifier_parser = identifier_parser
        self.__table_names_parser = table_names_parser

    def prepare(self, identifier: str):
        identifiers = self.__identifier_parser.parse(identifier)

        return self.__table_names_parser.parse(identifiers)
