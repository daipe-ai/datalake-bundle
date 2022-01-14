# pylint: disable = import-outside-toplevel
import unittest
from datalakebundle.table.schema.DiffGenerator import DiffGenerator


class DiffGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.__diff_generator = DiffGenerator()

    def test_no_diff(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema import df_schema
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array2 import df_schema_array2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array_reordered_changed_case import (
            df_schema_array_reordered_changed_case,
        )

        self.assertListEqual(self.__diff_generator.generate(df_schema, df_schema), [])
        self.assertListEqual(self.__diff_generator.generate(df_schema_array2, df_schema_array2), [])
        self.assertListEqual(self.__diff_generator.generate(df_schema_array2, df_schema_array_reordered_changed_case), [])

    def test_changed_attributes(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema import df_schema
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema import expected_schema
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema2 import expected_schema2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema3 import expected_schema3

        self.assertListEqual(self.__diff_generator.generate(df_schema, expected_schema), ["FIELD2['type'] changed from DATE to STRING"])

        self.assertListEqual(
            self.__diff_generator.generate(df_schema, expected_schema2), ["STRUCT1.NESTED_FIELD1['type'] changed from DATE to STRING"]
        )

        self.assertListEqual(
            self.__diff_generator.generate(df_schema, expected_schema3),
            ["STRUCT1.STRUCT2.NESTED_FIELD2['type'] changed from DATE to STRING"],
        )

    def test_added_columns_to_df_schema(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema2 import df_schema2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema4 import expected_schema4
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema3 import df_schema3
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema5 import expected_schema5

        self.assertListEqual(self.__diff_generator.generate(df_schema2, expected_schema4), ["root unexpected field: FIELD3"])

        self.assertListEqual(
            self.__diff_generator.generate(df_schema3, expected_schema4),
            [
                "STRUCT1['name'] changed from STRUCT1 to FIELD3",
                "STRUCT1['type'] changed from array to string",
                "root unexpected field: STRUCT1",
            ],
        )

        self.assertListEqual(
            self.__diff_generator.generate(df_schema3, expected_schema5),
            [
                "STRUCT1 unexpected field: STRUCT3",
                "STRUCT1.STRUCT2 missing field: NESTED_FIELD0",
                "STRUCT1.STRUCT2 missing field: NESTED_FIELD00",
            ],
        )

    def test_completely_different_schemas(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema4 import df_schema4
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema6 import expected_schema6

        self.assertListEqual(
            self.__diff_generator.generate(df_schema4, expected_schema6),
            [
                "FIELD6['name'] changed from FIELD6 to FIELD1",
                "FIELD6['type'] changed from struct (STRUCT1) to integer",
            ],
        )

    def test_schemas_with_arrays(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array import df_schema_array
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema_array import expected_schema_array
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array2 import df_schema_array2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array3 import df_schema_array3
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema_array2 import expected_schema_array2

        self.assertListEqual(
            self.__diff_generator.generate(df_schema_array, expected_schema_array),
            ["ARRAY1['elementType'] changed from array to string"],
        )
        self.assertListEqual(
            self.__diff_generator.generate(df_schema_array2, expected_schema_array),
            ["ARRAY1.array['elementType'] changed from string to array"],
        )
        self.assertListEqual(
            self.__diff_generator.generate(df_schema_array3, expected_schema_array2),
            [
                "ARRAY1.array['type'] changed from array to struct",
                "Unexpected field ARRAY1.array.struct",
                "Missing field ARRAY1.array.array",
            ],
        )

    def test_ignore_nullable(self):
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema import df_schema
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_nullable import df_schema_nullable
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array2 import df_schema_array2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_array_nullable2 import df_schema_array_nullable2
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.df_schema_nullable3 import df_schema_nullable3
        from datalakebundle.table.schema.DiffGeneratorTest_stubs.expected_schema_nullable3 import expected_schema_nullable3

        self.assertListEqual(self.__diff_generator.generate(df_schema, df_schema_nullable), [])
        self.assertListEqual(self.__diff_generator.generate(df_schema_array2, df_schema_array_nullable2), [])
        self.assertListEqual(
            self.__diff_generator.generate(df_schema_nullable3, expected_schema_nullable3),
            ["STRUCT1.['nullable']['name'] changed from ['NULLABLE'] to STRUCT2", "root missing field: NOT_NULLABLE"],
        )


if __name__ == "__main__":
    unittest.main()
