from consolebundle.ConsoleBundle import ConsoleBundle
from databricksbundle.DatabricksBundle import DatabricksBundle
from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.container.ContainerInterface import ContainerInterface
from injecta.package.pathResolver import resolvePath
from typing import List
from loggerbundle.LoggerBundle import LoggerBundle
from pyfony.kernel.BaseKernel import BaseKernel
from pyfonybundles.Bundle import Bundle
from datalakebundle.DataLakeBundle import DataLakeBundle

def initContainer(appEnv) -> ContainerInterface:
    class Kernel(BaseKernel):

        def _registerBundles(self) -> List[Bundle]:
            return [
                LoggerBundle(),
                ConsoleBundle(),
                DatabricksBundle('spark_test.yaml'),
                DataLakeBundle()
            ]

    kernel = Kernel(
        appEnv,
        resolvePath('datalakebundle') + '/_config',
        YamlConfigReader()
    )

    return kernel.initContainer()
