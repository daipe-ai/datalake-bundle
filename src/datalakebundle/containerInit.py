from consolebundle.ConsoleBundle import ConsoleBundle
from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.container.ContainerInterface import ContainerInterface
from injecta.package.pathResolver import resolvePath
from typing import List
from pyfony.kernel.BaseKernel import BaseKernel
from pyfonybundles.Bundle import Bundle
from datalakebundle.DataLakeBundle import DataLakeBundle

def initContainer(appEnv) -> ContainerInterface:
    class Kernel(BaseKernel):

        def _registerBundles(self) -> List[Bundle]:
            return [
                ConsoleBundle(),
                DataLakeBundle()
            ]

    kernel = Kernel(
        appEnv,
        resolvePath('datalakebundle') + '/_config',
        YamlConfigReader()
    )

    return kernel.initContainer()
