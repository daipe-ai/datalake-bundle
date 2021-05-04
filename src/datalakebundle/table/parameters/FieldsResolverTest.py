import unittest
from datalakebundle.table.parameters.FieldsResolver import FieldsResolver


class FieldsResolverTest(unittest.TestCase):
    def setUp(self):
        self.__fields_resolver = FieldsResolver()

    def test_basic(self):
        result = self.__fields_resolver.resolve(
            {
                "db_identifier": "mydatabase",
                "table_identifier": "my_table",
            },
            {
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                }
            },
        )

        self.assertEqual(
            {
                "db_identifier": "mydatabase",
                "table_identifier": "my_table",
                "target_path": "/foo/bar/mydatabase/my_table.delta",
            },
            result,
        )

    def test_explicit_overriding_defaults(self):
        result = self.__fields_resolver.resolve(
            {
                "db_identifier": "mydatabase",
                "table_identifier": "my_table",
                "target_path": "/foo/bar/mydatabase/my_table_new2.delta",
            },
            {
                "target_path": {
                    "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                    "resolver_arguments": ["/foo/bar"],
                },
            },
        )

        self.assertEqual(
            {
                "db_identifier": "mydatabase",
                "table_identifier": "my_table",
                "target_path": "/foo/bar/mydatabase/my_table_new2.delta",
            },
            result,
        )

    def test_infinite_loop_detection(self):
        with self.assertRaises(Exception) as error:
            self.__fields_resolver.resolve(
                {},
                {
                    "target_path": {
                        "resolver_class": "datalakebundle.test.SimpleTargetPathResolver",
                        "resolver_arguments": ["/foo/bar"],
                    }
                },
            )

        self.assertEqual(
            "Infinite assignment loop detected. Check get_depending_fields() of datalakebundle.test.SimpleTargetPathResolver",
            str(error.exception),
        )


if __name__ == "__main__":
    unittest.main()
