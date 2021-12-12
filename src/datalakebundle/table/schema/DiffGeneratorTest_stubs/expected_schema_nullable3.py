import pyspark.sql.types as t

expected_schema_nullable3 = t.StructType(
    [
        t.StructField("FIELD1_nullable", t.IntegerType(), nullable=True),
        t.StructField("nullable", t.StringType(), nullable=False),
        t.StructField("not_nullable", t.StringType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.StringType(), nullable=True),
                    t.StructField(
                        "['nullable']",
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
