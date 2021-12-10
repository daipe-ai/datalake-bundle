import unittest
from datalakebundle.table.schema.TableSchema import TableSchema
from pyspark.sql import types as t


class TableSchemaTest(unittest.TestCase):
    def test_to_json(self):
        schema = self.__get_basic_schema()
        schema_json = schema.jsonValue()

        self.assertEqual({"name": "name", "type": "string", "nullable": False, "metadata": {}}, schema_json["fields"][0])
        self.assertEqual({"name": "date", "type": "date", "nullable": False, "metadata": {}}, schema_json["fields"][1])
        self.assertEqual({"name": "visits", "type": "integer", "nullable": True, "metadata": {}}, schema_json["fields"][2])

        # "struct" type is important for spark data loaders to work properly
        self.assertEqual("struct", schema_json["type"])

    def __get_basic_schema(self):
        return TableSchema(
            [
                t.StructField("name", t.StringType(), False),
                t.StructField("date", t.DateType(), False),
                t.StructField("visits", t.IntegerType(), True),
            ],
            primary_key=["name", "date"],
            partition_by=["name"],
        )


if __name__ == "__main__":
    unittest.main()
