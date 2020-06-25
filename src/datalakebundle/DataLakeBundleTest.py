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
            'dbName': 'prf_test_mydatabase',
            'targetPath': '/foo/bar/mydatabase/encrypted/mydatabase.delta',
            'dbNameWithoutEnv': 'mydatabase',
            'encrypted': True,
            'tableName': 'my_table',
            'schemaPath': 'datalakebundle.test.TestSchema'
        }, myTableConfig)

        self.assertEqual({
            'dbName': 'prf_test_mydatabase',
            'targetPath': '/foo/bar/mydatabase/plain/mydatabase.delta',
            'dbNameWithoutEnv': 'mydatabase',
            'encrypted': False,
            'tableName': 'another_table',
            'schemaPath': 'datalakebundle.test.AnotherSchema',
            'partitionBy': ['date']
        }, anotherTableConfig)

    def test_externalTables(self):
        externalTablesConfig = self.__container.getParameters().datalakebundle.externalTables

        myExternalTableConfig = externalTablesConfig['mydatabase_e.my_external_table'].to_dict()
        anotherExternalTableConfig = externalTablesConfig['mydatabase_p.another_external_table'].to_dict()

        self.assertEqual({
            'dbName': 'prf_test_mydatabase',
            'dbNameWithoutEnv': 'mydatabase',
            'encrypted': True,
            'tableName': 'my_external_table',
        }, myExternalTableConfig)

        self.assertEqual({
            'dbName': 'prf_test_mydatabase',
            'dbNameWithoutEnv': 'mydatabase',
            'encrypted': False,
            'tableName': 'another_external_table',
        }, anotherExternalTableConfig)

if __name__ == '__main__':
    unittest.main()
