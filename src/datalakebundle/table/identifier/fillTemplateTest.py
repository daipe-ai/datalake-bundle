import unittest
from datalakebundle.table.identifier.fillTemplate import fillTemplate

class fillTemplateTest(unittest.TestCase):

    def test_basic(self):
        values = {'name': 'Jiri', 'age': 34}

        result = fillTemplate('Hello {name}, I am {age} years old', values)

        self.assertEqual('Hello Jiri, I am 34 years old', result)

    def test_missingValue(self):
        with self.assertRaises(Exception) as error:
            values = {'name': 'Jiri'}

            fillTemplate('Hello {name}, I am {age} years old', values)

        self.assertEqual('Value for placeholder {age} not defined', str(error.exception))

if __name__ == '__main__':
    unittest.main()
