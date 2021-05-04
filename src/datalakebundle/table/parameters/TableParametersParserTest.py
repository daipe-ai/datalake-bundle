import unittest
from pyfonycore.bootstrap import bootstrapped_container
from datalakebundle.table.parameters.TableParameters import TableParameters
from datalakebundle.table.parameters.TableParametersParser import TableParametersParser


class TableParametersParserTest(unittest.TestCase):
    __table_parameters_parser: TableParametersParser

    def setUp(self):
        container = bootstrapped_container.init("test")

        self.__table_parameters_parser = container.get(TableParametersParser)

    def test_basic(self):
        result = self.__table_parameters_parser.parse(
            "mydatabase.my_table",
            {},
            {
                "target_path": "/foo/bar/mydatabase/my_table.delta",
            },
        )

        self.assertEqual(
            TableParameters(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                }
            ),
            result,
        )

    def test_defaults_only(self):
        result = self.__table_parameters_parser.parse(
            "mydatabase.my_table",
            {
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                },
            },
        )

        self.assertEqual(
            TableParameters(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                }
            ),
            result,
        )

    def test_explicit_overriding_defaults(self):
        result = self.__table_parameters_parser.parse(
            "mydatabase.my_table",
            {
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                },
            },
            {},
        )

        self.assertEqual(
            TableParameters(
                **{
                    "db_identifier": "mydatabase",
                    "table_identifier": "my_table",
                    "db_name": "test_mydatabase",
                    "table_name": "my_table",
                    "target_path": "/foo/bar/mydatabase/my_table.delta",
                }
            ),
            result,
        )


if __name__ == "__main__":
    unittest.main()
