from typing import Union
from pyspark.sql.types import StructType


class TableDefinition:
    def __init__(
        self,
        db_name: str,
        table_name: str,
        schema: StructType,
        primary_key: Union[list, str],
        partition_by: Union[list, str],
        target_path: str,
        tbl_properties: dict,
    ):
        if not isinstance(primary_key, list):
            raise Exception(f"Invalid primary key: {primary_key}")

        if not isinstance(partition_by, list):
            raise Exception(f"Invalid partition_by: {partition_by}")

        if not isinstance(tbl_properties, dict):
            raise Exception(f"Invalid tbl_properties: {tbl_properties}")

        self.__db_name = db_name
        self.__table_name = table_name
        self.__schema = schema
        self.__primary_key = primary_key
        self.__partition_by = partition_by
        self.__target_path = target_path
        self.__tbl_properties = tbl_properties

    @property
    def db_name(self):
        return self.__db_name

    @property
    def table_name(self):
        return self.__table_name

    @property
    def full_table_name(self):
        return self.__db_name + "." + self.__table_name

    @property
    def schema(self) -> StructType:
        return self.__schema

    @property
    def primary_key(self) -> list:
        return self.__primary_key

    @property
    def partition_by(self) -> list:
        return self.__partition_by

    @property
    def target_path(self):
        return self.__target_path

    @property
    def tbl_properties(self) -> dict:
        return self.__tbl_properties
