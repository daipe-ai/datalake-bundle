import unittest
from pathlib import Path
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigParser import TableConfigParser


class TableConfigParserTest(unittest.TestCase):
    def setUp(self):
        self.__table_config_parser = TableConfigParser("test_{identifier}")

    def test_basic(self):
        result = self.__table_config_parser.parse(
            "mydatabase.my_table",
            {
                "schema_loader": "datalakebundle.test.TestSchema",
                "target_path": "/foo/bar/mydatabase/my_table.delta",
                "notebook_module": "datalakebundle.foo.mydatabase.my_table.my_table",
                "partition_by": "mycolumn",
            },
        )

        self.assertEqual(
            TableConfig(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "identifier": "mydatabase.my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "schema_loader": "datalakebundle.test.TestSchema",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                    "notebook_module": "datalakebundle.foo.mydatabase.my_table.my_table",
                    "partition_by": ["mycolumn"],
                }
            ),
            result,
        )

        self.assertEqual(Path.cwd().joinpath("src/datalakebundle/foo/mydatabase/my_table/my_table.py"), result.notebook_path)

    def test_defaults_only(self):
        result = self.__table_config_parser.parse(
            "mydatabase.my_table",
            {},
            {
                "schema_loader": "datalakebundle.test.{db_identifier}.{table_identifier}.schema:get_schema",
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                },
                "notebook_module": "datalakebundle.foo.{db_identifier}.{table_identifier}.{table_identifier}",
            },
        )

        self.assertEqual(
            TableConfig(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "identifier": "mydatabase.my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "schema_loader": "datalakebundle.test.mydatabase.my_table.schema:get_schema",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                    "notebook_module": "datalakebundle.foo.mydatabase.my_table.my_table",
                }
            ),
            result,
        )

        self.assertEqual(Path.cwd().joinpath("src/datalakebundle/foo/mydatabase/my_table/my_table.py"), result.notebook_path)

    def test_explicit_overriding_defaults(self):
        result = self.__table_config_parser.parse(
            "mydatabase.my_table",
            {"schema_loader": "datalakebundle.test.mydatabase.my_table.schema2:get_schema", "partition_by": ["mycolumn"]},
            {
                "schema_loader": "datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema:get_schema",
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                },
                "notebook_module": "datalakebundle.foo.{db_identifier}.{table_identifier}.{table_identifier}",
            },
        )

        self.assertEqual(
            TableConfig(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "identifier": "mydatabase.my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "schema_loader": "datalakebundle.test.mydatabase.my_table.schema2:get_schema",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                    "notebook_module": "datalakebundle.foo.mydatabase.my_table.my_table",
                    "partition_by": ["mycolumn"],
                }
            ),
            result,
        )

        self.assertEqual(Path.cwd().joinpath("src/datalakebundle/foo/mydatabase/my_table/my_table.py"), result.notebook_path)


if __name__ == "__main__":
    unittest.main()
