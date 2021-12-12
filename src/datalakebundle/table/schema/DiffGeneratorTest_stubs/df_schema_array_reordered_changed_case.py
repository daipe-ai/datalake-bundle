import pyspark.sql.types as t

df_schema_array_reordered_changed_case = t.StructType(
    [
        t.StructField(
            "arRay1",
            t.ArrayType(
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("FIELD5", t.IntegerType()),
                            t.StructField("field4", t.IntegerType()),
                        ]
                    )
                )
            ),
        ),
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
    ],
)
