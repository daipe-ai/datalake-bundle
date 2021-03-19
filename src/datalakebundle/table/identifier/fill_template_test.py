import unittest
from datalakebundle.table.identifier.fill_template import fill_template


class fill_template_test(unittest.TestCase):  # noqa: N801
    def test_basic(self):
        values = {"name": "Jiri", "age": 34}

        result = fill_template("Hello {name}, I am {age} years old", values)

        self.assertEqual("Hello Jiri, I am 34 years old", result)

    def test_missing_value(self):
        with self.assertRaises(Exception) as error:
            values = {"name": "Jiri"}

            fill_template("Hello {name}, I am {age} years old", values)

        self.assertEqual("Value for placeholder {age} not defined", str(error.exception))


if __name__ == "__main__":
    unittest.main()
