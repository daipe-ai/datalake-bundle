from pyfonybundles.Bundle import Bundle
from datalakebundle.table.identifier.Expression import Expression

Expression.yaml_loader.add_constructor(Expression.yaml_tag, Expression.from_yaml)

class DataLakeBundle(Bundle):
    pass
