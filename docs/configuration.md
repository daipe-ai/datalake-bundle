## Using table-specific configuration

Besides the [configuration options provided by the databricks-bundle](https://github.com/bricksflow/databricks-bundle/blob/master/docs/configuration.md), with datalake-bundle,
you can also define **table specific configuration**:

```yaml
parameters:
  datalakebundle:
    tables:
      customer.my_table:
        params:
          testDataPath: '/foo/bar'
```

Code of the **customer/my_table.py** notebook:

```python
from logging import Logger
from datalakebundle.notebook.decorators import notebookFunction, tableParams

@notebookFunction(tableParams('customer.my_table').testDataPath)
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

The `tableParams('customer.my_table')` function call is a shortcut to using `%datalakebundle.tables."customer.my_table".params%` string parameter.

___

Next section: [Tables management](tables-management.md)
