import json
from logging import Logger
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

from datalakebundle.table.create.TableDefinition import TableDefinition
from deepdiff import DeepDiff


class MetadataChecker:
    def __init__(self, default_properties: List[str], logger: Logger, spark: SparkSession):
        self.__default_properties = default_properties
        self.__logger = logger
        self.__spark = spark

    def check(self, table_definition: TableDefinition):
        tables = self.__spark.sql(f"SHOW TABLES IN {table_definition.db_name} LIKE '{table_definition.table_name}'")

        if not tables.rdd.isEmpty():
            self.__check_partition_by(table_definition)
            self.__check_tbl_properties(table_definition)

    def __check_partition_by(self, table_definition: TableDefinition):
        partitions_df = self.__spark.sql(f"DESCRIBE TABLE {table_definition.full_table_name}")
        partitions_df = partitions_df.filter(col("col_name").contains("Part "))

        if not partitions_df.rdd.isEmpty():
            partitions_col = partitions_df.select("data_type").collect()
            partitions = [row.data_type for row in partitions_col]

            ddiff = DeepDiff(partitions, table_definition.partition_by)

            if ddiff:
                unexpected_keys: dict = ddiff.get("iterable_item_added")
                missing_keys: dict = ddiff.get("iterable_item_removed")
                values_changed: dict = ddiff.get("values_changed")

                extra = {}

                if unexpected_keys:
                    extra["unexpected_keys"] = [v for v in unexpected_keys.values()]

                if missing_keys:
                    extra["missing_keys"] = [v for v in missing_keys.values()]

                if values_changed:
                    extra["values_changed"] = [f'{v["old_value"]} changed to {v["new_value"]}' for v in values_changed.values()]

                self.__create_warning(
                    "Table partitioning does NOT match schema",
                    extra,
                    table_definition,
                )

    def __check_tbl_properties(self, table_definition: TableDefinition):
        properties_df = self.__spark.sql(f"SHOW TBLPROPERTIES {table_definition.full_table_name}")
        tbl_properties = {k: v for k, v in properties_df.rdd.map(lambda x: (x.key, x.value)).collect()}

        for prop in self.__default_properties:
            tbl_properties.pop(prop, None)

        ddiff = DeepDiff(tbl_properties, table_definition.tbl_properties)

        if ddiff:
            unexpected_keys: list = ddiff.get("dictionary_item_added")
            missing_keys: list = ddiff.get("dictionary_item_removed")
            values_changed: dict = ddiff.get("values_changed")

            extra = {}

            def remove_wrapper(key):
                return key[5:-1]

            if missing_keys:
                extra["missing_keys"] = [remove_wrapper(key) for key in missing_keys]

            if unexpected_keys:
                extra["unexpected_keys"] = [remove_wrapper(key) for key in unexpected_keys]

            if values_changed:
                extra["values_changed"] = {
                    remove_wrapper(key): value.get("new_value", "Not found") for key, value in values_changed.items()
                }

            self.__create_warning(
                "TBL_PROPERTIES do NOT match table schema",
                extra,
                table_definition,
            )

    def __create_warning(self, warning_message: str, extra: dict, table_definition: TableDefinition):
        extra["table"] = table_definition.full_table_name

        report = json.dumps(extra, indent=4).replace('"', "")

        self.__logger.warning(f"{warning_message}\n" f"{report}")
