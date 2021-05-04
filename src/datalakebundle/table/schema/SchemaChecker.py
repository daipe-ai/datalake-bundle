import json
from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from deepdiff import DeepDiff


class SchemaChecker:
    def __init__(self, logger: Logger):
        self.__logger = logger

    def check(self, df: DataFrame, full_table_name: str, schema: StructType):
        def remove_metadata(json_schema):
            for field in json_schema["fields"]:
                field["metadata"] = dict()

            return json_schema

        expected_schema_json = remove_metadata(schema.jsonValue())
        df_schema_json = remove_metadata(df.schema.jsonValue())

        expected_set = self.__schema_to_set(expected_schema_json)
        df_set = self.__schema_to_set(df_schema_json)

        ddiff = DeepDiff(expected_set, df_set, ignore_string_case=True)

        if ddiff:
            error_message = "Table and dataframe schemas do NOT match"

            extra = {
                "table": full_table_name,
            }

            if "set_item_removed" in ddiff:
                extra["missing_fields"] = self.__pretty_ordered_set_to_output(ddiff["set_item_removed"])

            if "set_item_added" in ddiff:
                extra["unexpected_fields"] = self.__pretty_ordered_set_to_output(ddiff["set_item_added"])

            report = json.dumps(extra, indent=4).replace('"', "")

            self.__logger.error(f"{error_message}\n{report}")

            raise Exception(error_message)

    def __schema_to_set(self, schema: {}):
        return set([(f["name"], f["type"]) for f in schema["fields"]])

    def __pretty_ordered_set_to_output(self, pos):
        cols = [i[5:-1] for i in pos]
        return cols
