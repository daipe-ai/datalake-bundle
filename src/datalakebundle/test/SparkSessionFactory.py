from pyspark.sql.session import SparkSession

class SparkSessionFactory:

    def create(self) -> SparkSession:
        return SparkSession \
            .builder \
            .getOrCreate()
