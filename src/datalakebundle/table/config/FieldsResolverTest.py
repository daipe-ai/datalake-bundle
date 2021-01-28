import unittest
from datalakebundle.table.config.FieldsResolver import FieldsResolver

class FieldsResolverTest(unittest.TestCase):

    def setUp(self):
        self.__fieldsResolver = FieldsResolver()

    def test_basic(self):
        result = self.__fieldsResolver.resolve(
            {
                'dbIdentifier': 'mydatabase',
                'tableIdentifier': 'my_table',
                'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema:getSchema',
            },
            {
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
            'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema:getSchema',
            'targetPath': '/foo/bar/mydatabase/my_table.delta',
        }, result)

    def test_explicitOverridingDefaults(self):
        result = self.__fieldsResolver.resolve(
            {
                'dbIdentifier': 'mydatabase',
                'tableIdentifier': 'my_table',
                'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema2:getSchema',
                'targetPath': '/foo/bar/mydatabase/my_table_new2.delta',
            },
            {
                'schemaLoader': 'datalakebundle.test.{dbIdentifier}.{tableIdentifier}.schema:getSchema',
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
            'schemaLoader': 'datalakebundle.test.mydatabase.my_table.schema2:getSchema',
            'targetPath': '/foo/bar/mydatabase/my_table_new2.delta',
        }, result)

    def test_infiniteLoopDetection(self):
        with self.assertRaises(Exception) as error:
            self.__fieldsResolver.resolve(
                {},
                {
                    'targetPath': {
                        'resolverClass': 'datalakebundle.test.SimpleTargetPathResolver',
                        'resolverArguments': [
                            '/foo/bar'
                        ],
                    }
                }
            )

        self.assertEqual('Infinite assignment loop detected. Check getDependingFields() of datalakebundle.test.SimpleTargetPathResolver', str(error.exception))

if __name__ == '__main__':
    unittest.main()
