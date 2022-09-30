from typing import Optional
from datalakebundle.table.parameters import primitive_value
from datalakebundle.table.parameters.FieldsResolver import FieldsResolver
from datalakebundle.table.parameters.TableParameters import TableParameters
from datalakebundle.table.name.TableNamesPreparer import TableNamesPreparer


class TableParametersParser:

    __base_fields = ["identifier", "db_identifier", "db_name", "table_identifier", "table_name", "target_path"]

    def __init__(self, table_names_preparer: TableNamesPreparer):
        self.__fields_resolver = FieldsResolver()
        self.__table_names_preparer = table_names_preparer

    def parse(self, table_name_template: str, identifier: str, defaults: Optional[dict] = None, explicit_parameters: Optional[dict] = None):
        defaults = defaults or {}
        explicit_parameters = explicit_parameters or {}
        table_names = self.__table_names_preparer.prepare(table_name_template, identifier)

        all_fields = {**table_names.to_dict(), **explicit_parameters}

        for name, val in self.__filter_primitive(all_fields, defaults).items():
            all_fields[name] = primitive_value.evaluate(val, all_fields)

        all_fields = self.__fields_resolver.resolve(all_fields, defaults)

        return TableParameters(
            table_names.db_identifier,
            all_fields["db_name"],
            table_names.table_identifier,
            all_fields["table_name"],
            all_fields["target_path"],
            **{k: v for k, v in all_fields.items() if k not in self.__base_fields}
        )

    def __filter_primitive(self, all_fields: dict, defaults: dict):
        return {name: val for name, val in defaults.items() if not isinstance(val, dict) and name not in all_fields}
