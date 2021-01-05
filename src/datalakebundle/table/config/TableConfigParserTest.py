import unittest
from pathlib import Path
from datalakebundle.table.config.TableConfig import TableConfig
from datalakebundle.table.config.TableConfigParser import TableConfigParser

class TableConfigParserTest(unittest.TestCase):

    def setUp(self):
        self.__tableConfigParser = TableConfigParser('test_{identifier}')

    def test_basic(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {
                'schemaLoader': 'datalakebundle.test.TestSchema',
                'targetPath': '/foo/bar/mydatabase/my_table.delta',
                'notebookModule': 'datalakebundle.foo.mydatabase.my_table.my_table',
                'partitionBy': 'mycolumn',
            }
        )

        self.assertEqual(TableConfig(**{
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaLoader': 'datalakebundle.test.TestSchema',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
            'notebookModule': 'datalakebundle.foo.mydatabase.my_table.my_table',
            'partitionBy': ['mycolumn'],
        }), result)

        self.assertEqual(
            Path.cwd().joinpath('src/datalakebundle/foo/mydatabase/my_table/my_table.py'),
            result.notebookPath
        )

    def test_defaultsOnly(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {}, {
                'schemaLoader': 'datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema:getSchema',
                'targetPath': {
                    'resolverClass': 'datalakebundle.test.SimpleTargetPathResolver',
                    'resolverArguments': [
                        '/foo/bar'
                    ],
                },
                'notebookModule': 'datalakebundle.foo.{dbIdentifier}.{tableIdentifier}.{tableIdentifier}'
            }
        )

        self.assertEqual(TableConfig(**{
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema:getSchema',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
            'notebookModule': 'datalakebundle.foo.mydatabase.my_table.my_table'
        }), result)

        self.assertEqual(
            Path.cwd().joinpath('src/datalakebundle/foo/mydatabase/my_table/my_table.py'),
            result.notebookPath
        )

    def test_explicitOverridingDefaults(self):
        result = self.__tableConfigParser.parse(
            'mydatabase.my_table', {
                'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema2:getSchema',
                'partitionBy': ['mycolumn']
            }, {
                'schemaLoader': 'datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema:getSchema',
                'targetPath': {
                    'resolverClass': 'datalakebundle.test.SimpleTargetPathResolver',
                    'resolverArguments': [
                        '/foo/bar'
                    ],
                },
                'notebookModule': 'datalakebundle.foo.{dbIdentifier}.{tableIdentifier}.{tableIdentifier}'
            }
        )

        self.assertEqual(TableConfig(**{
            'dbIdentifier': 'mydatabase',
            'tableIdentifier': 'my_table',
            'identifier': 'mydatabase.my_table',
            'dbName': 'test_mydatabase',
            'tableName': 'my_table',
            'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema2:getSchema',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
            'notebookModule': 'datalakebundle.foo.mydatabase.my_table.my_table',
            'partitionBy': ['mycolumn'],
        }), result)

        self.assertEqual(
            Path.cwd().joinpath('src/datalakebundle/foo/mydatabase/my_table/my_table.py'),
            result.notebookPath
        )

if __name__ == '__main__':
    unittest.main()
