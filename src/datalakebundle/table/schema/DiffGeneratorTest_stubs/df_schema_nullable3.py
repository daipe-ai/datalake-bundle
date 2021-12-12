import pyspark.sql.types as t

df_schema_nullable3 = t.StructType(
    [
        t.StructField("FIELD1_nullable", t.IntegerType(), nullable=False),
        t.StructField("nullable", t.StringType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.StringType(), nullable=False),
                    t.StructField(
                        "STRUCT2",
                        t.StructType(
                            [
                                t.StructField("NESTED_FIELD2", t.StringType(), nullable=False),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ],
)
