import pyspark.sql.types as t
from datalakebundle.table.schema.TableSchemaGenerator import TableSchemaGenerator

schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField("FIELD3", t.DoubleType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.StringType()),
                    t.StructField(
                        "STRUCT2",
                        t.StructType(
                            [
                                t.StructField("NESTED_FIELD2", t.StringType()),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ],
)

expected_result = """def get_schema():
    return TableSchema(
        [
            t.StructField("FIELD1", t.IntegerType()),
            t.StructField("FIELD2", t.DoubleType()),
            t.StructField("FIELD3", t.DoubleType()),
            t.StructField(
                "STRUCT1",
                t.StructType(
                    [
                        t.StructField("NESTED_FIELD1", t.StringType()),
                        t.StructField(
                            "STRUCT2",
                            t.StructType(
                                [
                                    t.StructField("NESTED_FIELD2", t.StringType()),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
        ],
        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)
        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)
        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)
    )
"""

assert TableSchemaGenerator().generate(schema) == expected_result

schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField("FIELD3", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(
                t.StructType(
                    [
                        t.StructField("NESTED_ARRAY_FIELD1", t.StringType()),
                        t.StructField("NESTED_ARRAY_FIELD2", t.StringType()),
                        t.StructField("NESTED_ARRAY_FIELD3", t.ArrayType(t.StringType())),
                    ],
                ),
            ),
        ),
    ],
)

expected_result = """def get_schema():
    return TableSchema(
        [
            t.StructField("FIELD1", t.IntegerType()),
            t.StructField("FIELD2", t.DoubleType()),
            t.StructField("FIELD3", t.DoubleType()),
            t.StructField(
                "ARRAY1",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("NESTED_ARRAY_FIELD1", t.StringType()),
                            t.StructField("NESTED_ARRAY_FIELD2", t.StringType()),
                            t.StructField("NESTED_ARRAY_FIELD3", t.ArrayType(t.StringType())),
                        ],
                    ),
                ),
            ),
        ],
        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)
        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)
        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)
    )
"""

assert TableSchemaGenerator().generate(schema) == expected_result

schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField("FIELD3", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(
                t.ArrayType(t.StringType()),
            ),
        ),
    ],
)

expected_result = """def get_schema():
    return TableSchema(
        [
            t.StructField("FIELD1", t.IntegerType()),
            t.StructField("FIELD2", t.DoubleType()),
            t.StructField("FIELD3", t.DoubleType()),
            t.StructField(
                "ARRAY1",
                t.ArrayType(
                    t.ArrayType(t.StringType()),
                ),
            ),
        ],
        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)
        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)
        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)
    )
"""

assert TableSchemaGenerator().generate(schema) == expected_result

schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField("FIELD3", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField(
                                "VERY_BADLY_NESTED_ARRAY_OF_ARRAY_OF_ARRAY_OF_DOUBLES",
                                t.ArrayType(
                                    t.ArrayType(
                                        t.ArrayType(t.DoubleType()),
                                    ),
                                ),
                            ),
                        ],
                    ),
                ),
            ),
        ),
    ],
)

expected_result = """def get_schema():
    return TableSchema(
        [
            t.StructField("FIELD1", t.IntegerType()),
            t.StructField("FIELD2", t.DoubleType()),
            t.StructField("FIELD3", t.DoubleType()),
            t.StructField(
                "ARRAY1",
                t.ArrayType(
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField(
                                    "VERY_BADLY_NESTED_ARRAY_OF_ARRAY_OF_ARRAY_OF_DOUBLES",
                                    t.ArrayType(
                                        t.ArrayType(
                                            t.ArrayType(t.DoubleType()),
                                        ),
                                    ),
                                ),
                            ],
                        ),
                    ),
                ),
            ),
        ],
        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)
        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)
        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)
    )
"""

assert TableSchemaGenerator().generate(schema) == expected_result
