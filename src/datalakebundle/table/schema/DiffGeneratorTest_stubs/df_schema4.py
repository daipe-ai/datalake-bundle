import pyspark.sql.types as t

df_schema4 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
    ]
)
