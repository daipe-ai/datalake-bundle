from datalakebundle.table.identifier.Expression import Expression
from datalakebundle.table.identifier.fillTemplate import fillTemplate

def evaluate(val, rawTableConfig: dict):
    if isinstance(val, Expression):
        return val.evaluate(rawTableConfig)

    if isinstance(val, str):
        return fillTemplate(val, rawTableConfig)

    return val
