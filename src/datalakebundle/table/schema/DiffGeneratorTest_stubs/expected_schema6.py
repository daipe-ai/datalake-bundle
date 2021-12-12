import pyspark.sql.types as t

expected_schema6 = t.StructType(
    [
        t.StructField(
            "FIELD6",
            t.StructField(
                "STRUCT1",
                t.StructType(
                    [
                        t.StructField("NESTED_FIELD0", t.StringType()),
                    ],
                ),
            ),
        ),
    ]
)
