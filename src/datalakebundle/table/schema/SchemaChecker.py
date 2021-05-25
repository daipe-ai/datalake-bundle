import json
from logging import Logger

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from deepdiff import DeepDiff
import pprint

from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.schema.MetadataChecker import MetadataChecker


class SchemaChecker:
    def __init__(self, logger: Logger, metadata_checker: MetadataChecker):
        self.__logger = logger
        self.__metadata_checker = metadata_checker

    def check(self, df: DataFrame, full_table_name: str, table_definition: TableDefinition):
        extra = {"table": full_table_name, "diff": self.generate_diff(df.schema, table_definition.schema)}

        if extra["diff"]:
            error_message = "Table and dataframe schemas do NOT match"
            report = json.dumps(extra, indent=4).replace('"', "")

            self.__logger.error(f"{error_message}\n{report}")
            raise Exception(error_message)

        self.__metadata_checker.check(table_definition)

    def generate_diff(self, df_schema: StructType, schema: StructType):
        def remove_metadata(json_schema):
            for field in json_schema["fields"]:
                field["metadata"] = dict()

            return json_schema

        expected_schema = remove_metadata(schema.jsonValue())
        df_schema = remove_metadata(df_schema.jsonValue())

        ddiff = DeepDiff(expected_schema, df_schema, ignore_string_case=True, ignore_order=True)

        result = []
        if ddiff:

            if "values_changed" in ddiff:
                result.extend(self.__get_values_changed(expected_schema, ddiff["values_changed"]))

            if "type_changes" in ddiff:
                result.extend(self.__get_types_changed(expected_schema, ddiff["type_changes"]))

            if "iterable_item_added" in ddiff:
                result.extend(self.__get_iterable_item(expected_schema, ddiff["iterable_item_added"], "unexpected field"))

            if "iterable_item_removed" in ddiff:
                result.extend(self.__get_iterable_item(expected_schema, ddiff["iterable_item_removed"], "missing field"))

            if "dictionary_item_added" in ddiff:
                result.extend(self.__get_dictionary_item(expected_schema, ddiff["dictionary_item_added"], "Unexpected field"))

            if "dictionary_item_removed" in ddiff:
                result.extend(self.__get_dictionary_item(expected_schema, ddiff["dictionary_item_removed"], "Missing field"))

        print(result)
        return result

    def __rec_field_names(self, current: dict, chnks: list):
        if chnks:
            ch = chnks[0]
            if len(ch) == 1 and len(chnks) == 1:
                return
            if ch[0] == "elementType":
                yield "array"
                yield from self.__rec_field_names(current["elementType"], chnks[1:])
            else:
                new = current[ch[0]][int(ch[1])]
                yield new["name"]
                yield from self.__rec_field_names(new["type"], chnks[1:])

    def __chunks(self, lst: list, n: int):
        if not lst:
            return
        if lst[0] in ["containsNull", "elementType"]:
            yield [lst[0]]
            yield from self.__chunks(lst[1:], n)
        else:
            yield lst[0:n]
            yield from self.__chunks(lst[n:], n)

    def __get_field_path(self, expected_schema, k, cut_last_chunk=False):
        path = k[4:].replace("'", "").replace("[", "").split("]")[:-1]
        chnks = list(self.__chunks(path, 3))

        if cut_last_chunk:
            chnks.pop()
        field_names = self.__rec_field_names(expected_schema, chnks)
        return ".".join(field_names), path[-1]

    def __get_values_changed(self, expected_schema, values_changed):
        result = []

        for k, v in values_changed.items():
            field_path, attr = self.__get_field_path(expected_schema, k)

            ov = v["old_value"]
            nv = v["new_value"]
            if isinstance(ov, str) and ov not in ["struct", "array"]:
                ov = ov.upper()
            if isinstance(nv, str) and nv not in ["struct", "array"]:
                nv = nv.upper()
            if attr.isnumeric():
                attr = ""
            else:
                attr = f"['{attr}']"

            if isinstance(ov, dict):
                ov = pprint.pformat(ov, indent=4)
            if isinstance(nv, dict):
                nv = pprint.pformat(nv, indent=4)

            result.append(f"{field_path}{attr} changed from {ov} to {nv}")

        return result

    def __get_types_changed(self, expected_schema, values_changed):
        result = []

        for k, v in values_changed.items():
            field_path, attr = self.__get_field_path(expected_schema, k)

            if not field_path:
                field_path = "root"

            ov = v["old_value"]
            nv = v["new_value"]

            if isinstance(ov, dict):
                if "name" in ov:
                    ov = f"struct ({ov['name']})"
                else:
                    ov = "array"
            if isinstance(nv, dict):
                if "name" in nv:
                    nv = f"struct ({nv['name']})"
                else:
                    nv = "array"

            result.append(f"{field_path}['{attr}'] changed from {ov} to {nv}")
        return result

    def __get_iterable_item(self, expected_schema, iterable_items, message):
        result = []

        for k, v in iterable_items.items():
            field_path, attr = self.__get_field_path(expected_schema, k, True)
            if not field_path:
                field_path = "root"
            result.append(f'{field_path} {message}: {v["name"].upper()}')
        return result

    def __get_dictionary_item(self, expected_schema, dictionary_items, message):
        result = []

        for k in dictionary_items:
            field_path, attr = self.__get_field_path(expected_schema, k)

            if not field_path:
                field_path = "root"

            if attr == "fields":
                attr = ".struct"
            elif attr in ["elementType", "containsNull"]:
                attr = ".array"
            else:
                attr = f"[{attr}]"

            result.append(f"{message} {field_path}{attr}")
        return list(dict.fromkeys(result))
