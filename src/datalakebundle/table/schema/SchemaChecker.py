import json
import pprint
import re

from deepdiff import DeepDiff
from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.MetadataChecker import MetadataChecker


class SchemaChecker:
    def __init__(self, logger: Logger, metadata_checker: MetadataChecker):
        self.__logger = logger
        self.__metadata_checker = metadata_checker

    def check_with_metadata(self, df: DataFrame, table_definition: TableDefinition):
        self.check(df, table_definition)
        self.check_metadata(table_definition)

    def check(self, df: DataFrame, table_definition: TableDefinition):
        extra = {"table": table_definition.full_table_name, "diff": self.generate_diff(df.schema, table_definition.schema)}

        if extra["diff"]:
            error_message = "Table and dataframe schemas do NOT match"
            report = json.dumps(extra, indent=4).replace('"', "")

            self.__logger.error(f"{error_message}\n{report}")
            raise Exception(error_message)

    def check_metadata(self, table_definition: TableDefinition):
        self.__metadata_checker.check(table_definition)

    def generate_diff(self, df_schema: StructType, schema: StructType):
        def remove_metadata(json_schema):
            for field in json_schema["fields"]:
                field["metadata"] = dict()

            return json_schema

        expected_schema = remove_metadata(schema.jsonValue())
        df_schema = remove_metadata(df_schema.jsonValue())

        exclude_nullable = re.compile(r"\['nullable'\]")
        ddiff = DeepDiff(expected_schema, df_schema, ignore_string_case=True, ignore_order=True, exclude_regex_paths=[exclude_nullable])

        result = []
        if ddiff:

            if "values_changed" in ddiff:
                result.extend(self.__get_changed(expected_schema, ddiff["values_changed"]))

            if "type_changes" in ddiff:
                result.extend(self.__get_changed(expected_schema, ddiff["type_changes"], is_values=False))

            if "iterable_item_added" in ddiff:
                result.extend(self.__get_iterable_item(expected_schema, ddiff["iterable_item_added"], "unexpected field"))

            if "iterable_item_removed" in ddiff:
                result.extend(self.__get_iterable_item(expected_schema, ddiff["iterable_item_removed"], "missing field"))

            if "dictionary_item_added" in ddiff:
                result.extend(self.__get_dictionary_item(expected_schema, ddiff["dictionary_item_added"], "Unexpected field"))

            if "dictionary_item_removed" in ddiff:
                result.extend(self.__get_dictionary_item(expected_schema, ddiff["dictionary_item_removed"], "Missing field"))

        return result

    def __get_field_names(self, current: dict, chnks: list):
        if chnks:
            ch = chnks[0]
            if len(ch) == 1 and len(chnks) == 1:
                return
            if ch[0] == "elementType":
                yield "array"
                yield from self.__get_field_names(current["elementType"], chnks[1:])
            else:
                new = current[ch[0]][int(ch[1])]
                yield new["name"]
                yield from self.__get_field_names(new["type"], chnks[1:])

    def __get_field_path(self, expected_schema: dict, key: str, cut_last_chunk: bool = False):
        path = key[4:].replace("'", "").replace("[", "").split("]")[:-1]

        def chunks(lst: list, n: int):
            if not lst:
                return
            if lst[0] in ["containsNull", "elementType"]:
                yield [lst[0]]
                yield from chunks(lst[1:], n)
            else:
                yield lst[0:n]
                yield from chunks(lst[n:], n)

        chnks = list(chunks(path, 3))

        if cut_last_chunk:
            chnks.pop()

        field_names = self.__get_field_names(expected_schema, chnks)
        return ".".join(field_names), path[-1]

    def __get_changed(self, expected_schema: dict, values_changed: dict, is_values: bool = True):
        result = []

        for key, val in values_changed.items():
            field_path, last_field = self.__get_field_path(expected_schema, key)

            old_value = val["old_value"]
            new_value = val["new_value"]

            if is_values:
                old_value, new_value, last_field = self.__values_changed(old_value, new_value, last_field)
            else:
                old_value, new_value, last_field = self.__types_changed(old_value, new_value, last_field)

            result.append(f"{field_path}{last_field} changed from {old_value} to {new_value}")

        return result

    def __values_changed(self, old_value, new_value, last_field):
        if isinstance(old_value, str) and old_value not in ["struct", "array"]:
            old_value = old_value.upper()
        if isinstance(new_value, str) and new_value not in ["struct", "array"]:
            new_value = new_value.upper()
        if last_field.isnumeric():
            last_field = ""
        else:
            last_field = f"['{last_field}']"

        if isinstance(old_value, dict):
            old_value = pprint.pformat(old_value, indent=4)
        if isinstance(new_value, dict):
            new_value = pprint.pformat(new_value, indent=4)

        return old_value, new_value, last_field

    def __types_changed(self, old_value, new_value, last_field):
        if isinstance(old_value, dict):
            if "name" in old_value:
                old_value = f"struct ({old_value['name']})"
            else:
                old_value = "array"
        if isinstance(new_value, dict):
            if "name" in new_value:
                new_value = f"struct ({new_value['name']})"
            else:
                new_value = "array"

        return old_value, new_value, f"['{last_field}']"

    def __get_iterable_item(self, expected_schema: dict, iterable_items: dict, message: str):
        result = []

        for key, val in iterable_items.items():
            field_path, _ = self.__get_field_path(expected_schema, key, True)
            if not field_path:
                field_path = "root"
            result.append(f'{field_path} {message}: {val["name"].upper()}')
        return result

    def __get_dictionary_item(self, expected_schema: dict, dictionary_items: list, message: str):
        result = []

        for item in dictionary_items:
            field_path, last_field = self.__get_field_path(expected_schema, item)

            if not field_path:
                field_path = "root"

            if last_field == "fields":
                last_field = ".struct"
            elif last_field in ["elementType", "containsNull"]:
                last_field = ".array"
            else:
                last_field = f"[{last_field}]"

            result.append(f"{message} {field_path}{last_field}")
        return list(dict.fromkeys(result))
