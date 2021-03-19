import unittest
from pathlib import Path
from injecta.testing.services_tester import test_services
from pyfonycore.bootstrap import bootstrapped_container
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigManager import TableConfigManager


class DataLakeBundleTest(unittest.TestCase):
    def setUp(self):
        self._container = bootstrapped_container.init("test")
        self.max_diff = 8000 * 8

    def test_init(self):
        test_services(self._container)

    def test_table_configs(self):
        table_config_manager: TableConfigManager = self._container.get(TableConfigManager)

        my_table_config = table_config_manager.get("mydatabase_e.my_table")
        another_table_config = table_config_manager.get("mydatabase_p.another_table")

        self.assertEqual(
            TableConfig(
                **{
                    "schema_loader": "datalakebundle.test.TestSchema:get_schema",
                    "db_identifier": "mydatabase_e",
                    "table_identifier": "my_table",
                    "identifier": "mydatabase_e.my_table",
                    "notebook_module": "datalakebundle.mydatabase_e.my_table.my_table",
                    "db_name": "test_mydatabase_e",
                    "table_name": "my_table",
                    "encrypted": True,
                    "db_identifier_base": "mydatabase",
                    "target_path": "/foo/bar/mydatabase/encrypted/my_table.delta",
                }
            ),
            my_table_config,
        )

        self.assertEqual(Path.cwd().joinpath("src/datalakebundle/mydatabase_e/my_table/my_table.py"), my_table_config.notebook_path)

        self.assertEqual(
            TableConfig(
                **{
                    "schema_loader": "datalakebundle.test.AnotherSchema:get_schema",
                    "partition_by": ["date"],
                    "db_identifier": "mydatabase_p",
                    "table_identifier": "another_table",
                    "notebook_module": "datalakebundle.mydatabase_p.another_table.another_table",
                    "identifier": "mydatabase_p.another_table",
                    "db_name": "test_mydatabase_p",
                    "table_name": "another_table",
                    "encrypted": False,
                    "db_identifier_base": "mydatabase",
                    "target_path": "/foo/bar/mydatabase/plain/another_table.delta",
                }
            ),
            another_table_config,
        )

        self.assertEqual(
            Path.cwd().joinpath("src/datalakebundle/mydatabase_p/another_table/another_table.py"), another_table_config.notebook_path
        )


if __name__ == "__main__":
    unittest.main()
