from pyspark.sql.session import SparkSession


class TableExistenceChecker:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def table_exists(self, db_name: str, table_name: str) -> bool:
        return self.__spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []
