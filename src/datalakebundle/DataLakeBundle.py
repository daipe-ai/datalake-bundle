from pyfonybundles.Bundle import Bundle
from datalakebundle.table.identifier.Expression import Expression

Expression.yaml_loader.add_constructor(Expression.yaml_tag, Expression.from_yaml)


class DataLakeBundle(Bundle):
    def modify_raw_config(self, raw_config: dict) -> dict:
        table_defaults = raw_config["parameters"]["datalakebundle"]["table"]["defaults"]
        if table_defaults:
            self.__check_defaults(table_defaults)

        return raw_config

    def __check_defaults(self, table_defaults: dict):
        for field in ["identifier", "db_identifier", "db_name", "table_identifier", "table_name"]:
            if field in table_defaults:
                raise Exception(f"datalakebundle.table.defaults.{field} parameter must not be explicitly defined")
