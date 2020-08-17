import unittest
from injecta.testing.servicesTester import testServices
from datalakebundle.containerInit import initContainer

class DataLakeBundleTest(unittest.TestCase):

    def setUp(self) -> None:
        self.__container = initContainer('test')

    def test_init(self):
        testServices(self.__container)

    def test_tables(self):
        tablesConfig = self.__container.getParameters().datalakebundle.tables

        myTableConfig = tablesConfig['mydatabase_e.my_table'].to_dict()
        anotherTableConfig = tablesConfig['mydatabase_p.another_table'].to_dict()

        self.assertEqual({
            'schemaPath': 'datalakebundle.test.TestSchema',
            'dbIdentifier': 'mydatabase_e',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase_e.my_table',
            'dbName': 'test_mydatabase_e',
            'tableName': 'my_table',
            'encrypted': True,
            'dbIdentifierBase': 'mydatabase',
            'targetPath': '/foo/bar/mydatabase/encrypted/my_table.delta',
        }, myTableConfig)

        self.assertEqual({
            'schemaPath': 'datalakebundle.test.AnotherSchema',
            'partitionBy': ['date'],
            'dbIdentifier': 'mydatabase_p',
            'tableIdentifier': 'another_table',
            'identifier': 'mydatabase_p.another_table',
            'dbName': 'test_mydatabase_p',
            'tableName': 'another_table',
            'encrypted': False,
            'dbIdentifierBase': 'mydatabase',
            'targetPath': '/foo/bar/mydatabase/plain/another_table.delta',
        }, anotherTableConfig)

if __name__ == '__main__':
    unittest.main()
