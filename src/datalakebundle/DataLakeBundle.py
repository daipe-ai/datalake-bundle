from typing import List
from injecta.compiler.CompilerPassInterface import CompilerPassInterface
from pyfonybundles.Bundle import Bundle
from datalakebundle.table.config.TablesConfigCompilerPass import TablesConfigCompilerPass
from datalakebundle.table.identifier.Expression import Expression

Expression.yaml_loader.add_constructor(Expression.yaml_tag, Expression.from_yaml)

class DataLakeBundle(Bundle):

    def getCompilerPasses(self) -> List[CompilerPassInterface]:
        return [
            TablesConfigCompilerPass()
        ]
