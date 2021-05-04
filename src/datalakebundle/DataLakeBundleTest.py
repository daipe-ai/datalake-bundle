import unittest
from injecta.testing.services_tester import test_services
from pyfonycore.bootstrap import bootstrapped_container
from datalakebundle.table.parameters.TableParameters import TableParameters
from datalakebundle.table.parameters.TableParametersManager import TableParametersManager


class DataLakeBundleTest(unittest.TestCase):
    def setUp(self):
        self._container = bootstrapped_container.init("test")
        self.max_diff = 8000 * 8

    def test_init(self):
        test_services(self._container)

    def test_table_parameters(self):
        table_parameters_manager: TableParametersManager = self._container.get(TableParametersManager)

        my_table_parameters = table_parameters_manager.get_or_parse("mydatabase_e.my_table")
        another_table_parameters = table_parameters_manager.get_or_parse("mydatabase_p.another_table")

        self.assertEqual(
            TableParameters(
                **{
                    "db_identifier": "mydatabase_e",
                    "table_identifier": "my_table",
                    "identifier": "mydatabase_e.my_table",
                    "db_name": "test_mydatabase_e",
                    "table_name": "my_table",
                    "encrypted": True,
                    "db_identifier_base": "mydatabase",
                    "target_path": "/foo/bar/mydatabase/encrypted/my_table.delta",
                }
            ),
            my_table_parameters,
        )

        self.assertEqual(
            TableParameters(
                **{
                    "db_identifier": "mydatabase_p",
                    "table_identifier": "another_table",
                    "identifier": "mydatabase_p.another_table",
                    "db_name": "test_mydatabase_p",
                    "table_name": "another_table",
                    "encrypted": False,
                    "db_identifier_base": "mydatabase",
                    "target_path": "/foo/bar/mydatabase/plain/another_table.delta",
                }
            ),
            another_table_parameters,
        )


if __name__ == "__main__":
    unittest.main()
