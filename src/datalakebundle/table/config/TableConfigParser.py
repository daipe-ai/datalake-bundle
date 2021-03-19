from datalakebundle.table.config import primitive_value
from datalakebundle.table.config.FieldsResolver import FieldsResolver
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.identifier.fill_template import fill_template
from datalakebundle.table.identifier.IdentifierParser import IdentifierParser


class TableConfigParser:
    def __init__(self, table_name_template: str):
        self.__fields_resolver = FieldsResolver()
        self.__identifier_parser = IdentifierParser()
        self.__table_name_template = table_name_template

    def parse(self, identifier: str, explicit_config: dict, defaults: dict = None):
        defaults = defaults or dict()
        identifiers = self.__identifier_parser.parse(identifier)
        table_name_parts = self.__resolve_table_name_parts(identifiers)

        if "partition_by" in explicit_config and isinstance(explicit_config["partition_by"], str):
            explicit_config["partition_by"] = [explicit_config["partition_by"]]

        all_fields = {**identifiers, **table_name_parts, **explicit_config}

        for name, val in self.__filter_primitive(all_fields, defaults).items():
            all_fields[name] = primitive_value.evaluate(val, all_fields)

        all_fields = self.__fields_resolver.resolve(all_fields, defaults)

        return TableConfig(**all_fields)

    def __filter_primitive(self, all_fields: dict, defaults: dict):
        return {name: val for name, val in defaults.items() if not isinstance(val, dict) and name not in all_fields}

    def __resolve_table_name_parts(self, identifiers: dict):
        full_table_name = fill_template(self.__table_name_template, identifiers)
        dot_position = full_table_name.find(".")

        if dot_position == -1:
            raise Exception("Table name must meet the following format: {dbName}.{tableName}")

        return {
            "db_name": full_table_name[:dot_position],
            "table_name": full_table_name[dot_position + 1 :],  # noqa: E203
        }
