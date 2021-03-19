from datalakebundle.table.identifier.Expression import Expression
from datalakebundle.table.identifier.fill_template import fill_template


def evaluate(val, raw_table_config: dict):
    if isinstance(val, Expression):
        return val.evaluate(raw_table_config)

    if isinstance(val, str):
        return fill_template(val, raw_table_config)

    return val
