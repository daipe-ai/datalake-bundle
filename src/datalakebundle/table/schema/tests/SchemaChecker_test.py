import unittest
from logging import Logger

import pyspark.sql.types as t
from pyfonycore.bootstrap import bootstrapped_container

from datalakebundle.table.schema.MetadataChecker import MetadataChecker
from datalakebundle.table.schema.SchemaChecker import SchemaChecker

df_schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
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

expected_schema = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DateType()),
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


expected_schema2 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.DateType()),
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

expected_schema3 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.StringType()),
                    t.StructField(
                        "STRUCT2",
                        t.StructType(
                            [
                                t.StructField("NESTED_FIELD2", t.DateType()),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ],
)

df_schema2 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
        t.StructField("FIELD3", t.StringType()),
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

df_schema3 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
        t.StructField("FIELD3", t.StringType()),
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
                    t.StructField(
                        "STRUCT3",
                        t.StructType(
                            [
                                t.StructField("NESTED_FIELD3", t.StringType()),
                                t.StructField("NESTED_FIELD4", t.StringType()),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ],
)

expected_schema4 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
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

expected_schema5 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.StringType()),
        t.StructField("FIELD3", t.StringType()),
        t.StructField(
            "STRUCT1",
            t.StructType(
                [
                    t.StructField("NESTED_FIELD1", t.StringType()),
                    t.StructField(
                        "STRUCT2",
                        t.StructType(
                            [
                                t.StructField("NESTED_FIELD0", t.StringType()),
                                t.StructField("NESTED_FIELD00", t.StringType()),
                                t.StructField("NESTED_FIELD2", t.StringType()),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ],
)

df_schema4 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
    ]
)

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

df_schema_array = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(t.StringType()),
        ),
    ],
)

expected_schema_array = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(t.ArrayType(t.StringType())),
        ),
    ],
)

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

df_schema_array3 = t.StructType(
    [
        t.StructField("FIELD1", t.IntegerType()),
        t.StructField("FIELD2", t.DoubleType()),
        t.StructField(
            "ARRAY1",
            t.ArrayType(
                t.StructType(
                    [
                        t.StructField("FIELD4", t.IntegerType()),
                        t.StructField("FIELD5", t.IntegerType()),
                    ]
                )
            ),
        ),
    ],
)

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


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._container = bootstrapped_container.init("test")
        logger = Logger("test")
        mc: MetadataChecker = self._container.get(MetadataChecker)
        self.schema_checker = SchemaChecker(logger, mc)

    def test_no_diff(self):
        assert self.schema_checker.generate_diff(df_schema, df_schema) == []
        assert self.schema_checker.generate_diff(df_schema_array2, df_schema_array2) == []
        assert self.schema_checker.generate_diff(df_schema_array2, df_schema_array_reordered_changed_case) == []

    def test_changed_attributes(self):
        assert self.schema_checker.generate_diff(df_schema, expected_schema) == ["FIELD2['type'] changed from DATE to STRING"]

        assert self.schema_checker.generate_diff(df_schema, expected_schema2) == [
            "STRUCT1.NESTED_FIELD1['type'] changed from DATE to STRING"
        ]

        assert self.schema_checker.generate_diff(df_schema, expected_schema3) == [
            "STRUCT1.STRUCT2.NESTED_FIELD2['type'] changed from DATE to STRING"
        ]

    def test_added_columns_to_df_schema(self):
        assert self.schema_checker.generate_diff(df_schema2, expected_schema4) == ["root unexpected field: FIELD3"]

        assert self.schema_checker.generate_diff(df_schema3, expected_schema4) == [
            "STRUCT1['name'] changed from STRUCT1 to FIELD3",
            "STRUCT1['type'] changed from array to string",
            "root unexpected field: STRUCT1",
        ]

        assert self.schema_checker.generate_diff(df_schema3, expected_schema5) == [
            "STRUCT1 unexpected field: STRUCT3",
            "STRUCT1.STRUCT2 missing field: NESTED_FIELD0",
            "STRUCT1.STRUCT2 missing field: NESTED_FIELD00",
        ]

    def test_completely_different_schemas(self):
        assert self.schema_checker.generate_diff(df_schema4, expected_schema6) == [
            "FIELD6['name'] changed from FIELD6 to FIELD1",
            "FIELD6['type'] changed from struct (STRUCT1) to integer",
        ]

    def test_schemas_with_arrays(self):
        assert self.schema_checker.generate_diff(df_schema_array, expected_schema_array) == [
            "ARRAY1['elementType'] changed from array to string"
        ]
        assert self.schema_checker.generate_diff(df_schema_array2, expected_schema_array) == [
            "ARRAY1.array['elementType'] changed from string to array"
        ]
        assert self.schema_checker.generate_diff(df_schema_array3, expected_schema_array2) == [
            "ARRAY1.array['type'] changed from array to struct",
            "Unexpected field ARRAY1.array.struct",
            "Missing field ARRAY1.array.array",
        ]
