from typing import Union, Optional
from pyspark.sql.types import StructType


def _get_list_value(value, invalid_message: str):
    if value is None:
        return []

    if isinstance(value, str):
        return [value]

    if not isinstance(value, str) and not isinstance(value, list):
        raise Exception(invalid_message.format(value=value))

    return value


def _get_tbl_properties(tbl_properties):
    if tbl_properties is None:
        tbl_properties = tbl_properties or {}
    elif not isinstance(tbl_properties, dict):
        raise Exception(f"Invalid tbl_properties: {tbl_properties}")

    for k, v in tbl_properties.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise Exception(f"Invalid tbl_properties - keys and values not strings: {tbl_properties}")

    return tbl_properties


class TableSchema(StructType):
    def __init__(
        self,
        fields: list,
        primary_key: Optional[Union[str, list]] = None,
        partition_by: Optional[Union[str, list]] = None,
        tbl_properties: Optional[dict] = None,
    ):
        super().__init__(fields)

        self.__primary_key = _get_list_value(primary_key, "Invalid primary key: {value}")
        self.__partition_by = _get_list_value(partition_by, "Invalid partition by: {value}")
        self.__tbl_properties = _get_tbl_properties(tbl_properties)

    @property
    def primary_key(self) -> list:
        return self.__primary_key

    @property
    def partition_by(self) -> list:
        return self.__partition_by

    @property
    def tbl_properties(self) -> dict:
        return self.__tbl_properties

    @classmethod
    def typeName(cls):
        return "struct"
