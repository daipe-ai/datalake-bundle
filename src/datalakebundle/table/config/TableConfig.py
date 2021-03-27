import json
import os
from pathlib import Path
from injecta.package.path_resolver import resolve_path
from injecta.module import attribute_loader
import pyspark.sql.types as t


class TableConfig:
    def __init__(
        self,
        identifier: str,
        db_identifier: str,
        db_name: str,
        table_identifier: str,
        table_name: str,
        schema_loader: str,
        target_path: str,
        notebook_module: str,
        partition_by: list = None,
        **kwargs,
    ):
        self.__identifier = identifier
        self.__db_identifier = db_identifier
        self.__db_name = db_name
        self.__table_identifier = table_identifier
        self.__table_name = table_name
        self.__schema_loader = schema_loader
        self.__target_path = target_path
        self.__notebook_module = notebook_module
        self.__partition_by = partition_by or []
        self.__custom_fields = kwargs

    @property
    def identifier(self):
        return self.__identifier

    @property
    def db_identifier(self):
        return self.__db_identifier

    @property
    def db_name(self):
        return self.__db_name

    @property
    def table_identifier(self):
        return self.__table_identifier

    @property
    def table_name(self):
        return self.__table_name

    @property
    def schema_loader(self):
        return self.__schema_loader

    @property
    def schema(self) -> t.StructType:
        get_schema = attribute_loader.load_from_string(self.__schema_loader)

        return get_schema()

    @property
    def target_path(self):
        return self.__target_path

    @property
    def notebook_module(self):
        return self.__notebook_module

    @property
    def notebook_path(self) -> Path:
        parts = self.__notebook_module.split(".")
        base_path = resolve_path(parts[0])

        return Path(base_path + os.sep + os.sep.join(parts[1:]) + ".py")

    @property
    def partition_by(self):
        return self.__partition_by

    @property
    def full_table_name(self):
        return self.__db_name + "." + self.__table_name

    def get_custom_fields(self):
        return self.__custom_fields

    def as_dict(self):
        return {
            **self.__custom_fields,
            **{
                "identifier": self.__identifier,
                "db_identifier": self.__db_identifier,
                "db_name": self.__db_name,
                "table_identifier": self.__table_identifier,
                "table_name": self.__table_name,
                "schema_loader": self.__schema_loader,
                "target_path": self.__target_path,
                "notebook_module": self.__notebook_module,
                "partition_by": self.__partition_by,
            },
        }

    def __getattr__(self, item):
        if item not in self.__custom_fields:
            raise Exception(f'Unexpected attribute: "{item}"')

        return self.__custom_fields[item]

    def __eq__(self, other: "TableConfig"):
        return self.as_dict() == other.as_dict()

    def __repr__(self):
        return json.dumps(self.as_dict(), indent=4)
