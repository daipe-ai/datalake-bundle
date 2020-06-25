import unittest
from datalakebundle.table.identifier.IdentifierParser import IdentifierParser

class IdentifierParserTest(unittest.TestCase):

    def test_basic(self):
        identifierParser = IdentifierParser('{dbName}.{tableName}')

        result = identifierParser.parse('my_database.my_table')

        self.assertEqual('my_database', result['dbName'])
        self.assertEqual('my_table', result['tableName'])

    def test_extraAttributes(self):
        identifierParser = IdentifierParser('{dbName}_{encrypted}.{tableName}')

        result = identifierParser.parse('my_database_e.my_table')

        self.assertEqual('my_database', result['dbName'])
        self.assertEqual('e', result['encrypted'])
        self.assertEqual('my_table', result['tableName'])

if __name__ == '__main__':
    unittest.main()
