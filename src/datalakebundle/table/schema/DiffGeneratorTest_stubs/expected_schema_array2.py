import pyspark.sql.types as t

expected_schema_array2 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(t.ArrayType(t.StringType())),
        ),
    ],
)
