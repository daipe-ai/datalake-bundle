import unittest
from injecta.testing.servicesTester import testServices
from datalakebundle.containerInit import initContainer

class DataLakeBundleTest(unittest.TestCase):

    def test_init(self):
        container = initContainer('test')

        testServices(container)

if __name__ == '__main__':
    unittest.main()
