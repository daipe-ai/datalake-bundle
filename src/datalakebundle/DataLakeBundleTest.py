import os
import unittest
from consolebundle.ConsoleBundle import ConsoleBundle
from injecta.testing.servicesTester import testServices
from injecta.config.YamlConfigReader import YamlConfigReader
from typing import List
from pyfony.PyfonyBundle import PyfonyBundle
from pyfony.kernel.BaseKernel import BaseKernel
from pyfonybundles.Bundle import Bundle
from datalakebundle.DataLakeBundle import DataLakeBundle

class DataLakeBundleTest(unittest.TestCase):

    def test_init(self):
        class Kernel(BaseKernel):

            def _registerBundles(self) -> List[Bundle]:
                return [
                    PyfonyBundle(),
                    ConsoleBundle(),
                    DataLakeBundle()
                ]

        kernel = Kernel(
            'test',
            os.getcwd() + '/DataLakeBundleTest',
            YamlConfigReader()
        )

        container = kernel.initContainer()

        testServices(container)

if __name__ == '__main__':
    unittest.main()
