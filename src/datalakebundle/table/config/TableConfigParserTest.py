import unittest
from datalakebundle.containerInit import initContainer
from datalakebundle.table.config.TableConfigParser import TableConfigParser

class TableConfigParserTest(unittest.TestCase):

    def setUp(self) -> None:
        container = initContainer('test')

        self.__bundleConfig = container.getParameters().datalakebundle
        self.__tableConfigParser = TableConfigParser('test_{identifier}')

    def test_basic(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {
                'schemaPath': 'datalakebundle.test.TestSchema',
            }
        )

        self.assertEqual({
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaPath': 'datalakebundle.test.TestSchema',
        }, result)

    def test_defaultsOnly(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {}, {
                'schemaPath': 'datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema',
                'targetPath': {
                    'resolverClass': 'datalakebundle.test.SimpleTargetPathResolver',
                    'resolverArguments': [
                        '/foo/bar'
                    ],
                }
            }
        )

        self.assertEqual({
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaPath': 'datalakebundle.test.mydatabase.my_table.schema',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
        }, result)

    def test_explicitOverridingDefaults(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {
                'schemaPath': 'datalakebundle.test.mydatabase.my_table.schema2',
            }, {
                'schemaPath': 'datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema',
                'targetPath': {
                    'resolverClass': 'datalakebundle.test.SimpleTargetPathResolver',
                    'resolverArguments': [
                        '/foo/bar'
                    ],
                }
            }
        )

        self.assertEqual({
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaPath': 'datalakebundle.test.mydatabase.my_table.schema2',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
        }, result)

if __name__ == '__main__':
    unittest.main()
