import pyspark.sql.types as t

df_schema_array2 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("FIELD4", t.IntegerType()),
                            t.StructField("FIELD5", t.IntegerType()),
                        ]
                    )
                )
            ),
        ),
    ],
)
