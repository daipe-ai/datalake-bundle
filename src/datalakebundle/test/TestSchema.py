import pyspark.sql.types as t


def get_schema():
    return t.StructType(
        [
            t.StructField("name", t.StringType(), True),
            t.StructField("create_time", t.TimestampType(), True),
        ]
    )
