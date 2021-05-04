from datalakebundle.table.identifier.fill_template import fill_template
from datalakebundle.table.name.TableNames import TableNames


class TableNamesParser:
    def __init__(self, table_name_template: str):
        self.__table_name_template = table_name_template

    def parse(self, identifiers: dict):
        full_table_name = fill_template(self.__table_name_template, identifiers)
        dot_position = full_table_name.find(".")

        if dot_position == -1:
            raise Exception("Table name must meet the following format: {db_name}.{table_name}")

        return TableNames(
            identifiers["db_identifier"],
            full_table_name[:dot_position],
            identifiers["table_identifier"],
            full_table_name[dot_position + 1 :],  # noqa: E203,
        )
