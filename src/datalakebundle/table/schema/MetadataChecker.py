import json
from logging import Logger
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException

from datalakebundle.table.create.TableDefinition import TableDefinition
from deepdiff import DeepDiff


class MetadataChecker:
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def check(self, table_definition: TableDefinition):
        tables = self.__spark.sql(f"SHOW TABLES IN {table_definition.db_name}").toPandas()

        if tables["tableName"].str.contains(table_definition.table_name).any():
            self.__check_primary_key(table_definition)
            self.__check_partition_by(table_definition)
            self.__check_tbl_properties(table_definition)

    def __check_primary_key(self, table_definition: TableDefinition):
        pass

    def __check_partition_by(self, table_definition: TableDefinition):
        try:
            partitions_df = self.__spark.sql(f"SHOW PARTITIONS {table_definition.full_table_name}")
        except AnalysisException:
            self.__logger.info(f"Table {table_definition.full_table_name} is not partitioned.")
        else:
            ddiff = DeepDiff(partitions_df.columns, table_definition.partition_by)

            if ddiff:
                new_keys: dict = ddiff.get("iterable_item_added")
                values_changed: dict = ddiff.get("values_changed")

                extra = {}

                if new_keys:
                    extra["missing_keys"] = [v for v in list(new_keys.values())]

                if values_changed:
                    extra["values_changed"] = [f'{v["old_value"]} changed to {v["new_value"]}' for v in list(values_changed.values())]

                self.__create_warning(
                    "PARTITION_BY does NOT match schema",
                    extra,
                    table_definition,
                )

    def __check_tbl_properties(self, table_definition: TableDefinition):
        properties_df = self.__spark.sql(f"SHOW TBLPROPERTIES {table_definition.full_table_name}")
        properties_pd = properties_df.toPandas().set_index("key")
        tbl_properties = properties_pd.to_dict()["value"]

        tbl_properties.pop("Type", None)
        tbl_properties.pop("delta.minReaderVersion", None)
        tbl_properties.pop("delta.minWriterVersion", None)

        ddiff = DeepDiff(tbl_properties, table_definition.tbl_properties)

        if ddiff:
            new_keys: list = ddiff.get("dictionary_item_added")
            removed_keys: list = ddiff.get("dictionary_item_removed")
            values_changed: dict = ddiff.get("values_changed")

            extra = {}

            if new_keys:
                extra["missing_keys"] = [k[5:-1] for k in new_keys]

            if removed_keys:
                extra["unexpected_keys"] = [k[5:-1] for k in removed_keys]

            if values_changed:
                extra["values_changed"] = {k[5:-1]: v.get("new_value", "Not found") for k, v in values_changed.items()}

            self.__create_warning(
                "TBL_PROPERTIES do NOT match schema",
                extra,
                table_definition,
            )

    def __create_warning(self, warning_message: str, extra: dict, table_definition: TableDefinition):
        extra["table"] = table_definition.full_table_name

        report = json.dumps(extra, indent=4).replace('"', "")

        self.__logger.warning(f"{warning_message}\n" f"{report}")
