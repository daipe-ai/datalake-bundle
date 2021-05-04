from typing import Union
from pyspark.sql.types import StructType


class TableSchema(StructType):
    def __init__(
        self, fields: list, primary_key: Union[str, list] = None, partition_by: Union[str, list] = None, tbl_properties: dict = None
    ):
        primary_key = primary_key or []
        partition_by = partition_by or []
        tbl_properties = tbl_properties or {}

        if not isinstance(primary_key, str) and not isinstance(primary_key, list):
            raise Exception(f"Invalid primary key: {primary_key}")

        if not isinstance(partition_by, str) and not isinstance(partition_by, list):
            raise Exception(f"Invalid partition by: {partition_by}")

        if not isinstance(tbl_properties, dict):
            raise Exception(f"Invalid tbl_properties: {tbl_properties}")

        for k, v in tbl_properties.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise Exception(f"Invalid tbl_properties - keys and values not strings: {tbl_properties}")

        super().__init__(fields)

        self.__primary_key = [primary_key] if isinstance(primary_key, str) else primary_key
        self.__partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
        self.__tbl_properties = tbl_properties

    @property
    def primary_key(self) -> list:
        return self.__primary_key

    @property
    def partition_by(self) -> list:
        return self.__partition_by

    @property
    def tbl_properties(self) -> dict:
        return self.__tbl_properties
