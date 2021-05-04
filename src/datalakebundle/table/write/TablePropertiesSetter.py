from logging import Logger
from pyspark.sql.session import SparkSession
from datalakebundle.table.create.TableDefinition import TableDefinition


class TablePropertiesSetter:
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def set(self, table_definition: TableDefinition):
        if table_definition.tbl_properties:
            properties: str = ", ".join([f"'{k}'='{v}'" for k, v in table_definition.tbl_properties.items()])

            self.__logger.info(f"Setting TBLPROPERTIES=({properties})")

            self.__spark.sql(f"ALTER TABLE {table_definition.full_table_name} SET TBLPROPERTIES({properties})")
