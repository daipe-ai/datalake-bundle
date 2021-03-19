import unittest
from types import FunctionType


class DecoratorsTest(unittest.TestCase):
    def test_basic(self):
        from datalakebundle.notebook.decorator.tests.decorators_test import load_data

        result = load_data()

        self.assertIsInstance(load_data, FunctionType)
        self.assertEqual(155, result)

    def test_error(self):
        with self.assertRaises(Exception) as error:
            from datalakebundle.notebook.decorator.tests.decorators_fixture import load_data3  # noqa: F401

        self.assertEqual("Use @data_frame_loader() instead of @data_frame_loader please", str(error.exception))


if __name__ == "__main__":
    unittest.main()
