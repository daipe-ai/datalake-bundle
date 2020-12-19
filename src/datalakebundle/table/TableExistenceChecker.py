from pyspark.sql.session import SparkSession

class TableExistenceChecker:

    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def tableExists(self, dbName: str, tableName: str) -> bool:
        return self.__spark.sql(f'SHOW TABLES IN {dbName} LIKE "{tableName}"').collect() != []
