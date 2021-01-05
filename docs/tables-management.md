## Tables management

### Managing datalake tables using TableManager

`TableManager` defines set of commonly used methods to manage datalake tables.
The following example recreates the `my_crm.customers` table (delete old data, create new empty table) every time the notebook runs.

```python
from logging import Logger
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameSaver
from datalakebundle.table.TableManager import TableManager

@dataFrameSaver()
def customers_table(df: DataFrame, logger: Logger, tableManager: TableManager):
    logger.info('Recreating table my_crm.customers')

    tableManager.recreate('my_crm.customers')

    return df.insertInto(tableManager.getName('my_crm.customers'))
```

**All TableManager's methods**:

* `getName('my_crm.customers')` - returns final table name
* `getConfig('my_crm.customers')` - returns [TableConfig instance](../src/datalakebundle/table/config/TableConfig.py)
* `create('my_crm.customers')` - creates table
* `recreate('my_crm.customers')` - recreates (deletes Hive table, **deletes data**, create new empty table)
* `exists('my_crm.customers')` - checks if table exists
* `delete('my_crm.customers')` - deletes Hive table, **deletes data**
* `optimizeAll()` - runs `OPTIMIZE` command on all defined tables

### Managing datalake tables using the console commands

**Example**: To create the `customer.my_table` table in your datalake, just type `console datalake:table:create customer.my_table --env=dev`
into your terminal within your activated project's virtual environment.
The command connects to your cluster via [Databricks Connect](https://github.com/bricksflow/databricks-bundle/blob/master/docs/databricks-connect.md) and creates the table as configured.

All commands available in the datalake-bundle: 

* `datalake:table:create [table identifier]` - Creates a metastore table based on it's YAML definition (name, schema, data path, ...)

* `datalake:table:recreate [table identifier]` - Re-creates a metastore table based on it's YAML definition (name, schema, data path, ...)

* `datalake:table:delete [table identifier]` - Deletes a metastore table including data on HDFS

* `datalake:table:create-missing` - Creates newly defined tables that do not exist in the metastore yet

* `datalake:table:optimize-all` - Runs the OPTIMIZE command on all defined tables (Delta only)

___

[Parsing fields from table identifier](parsing-fields.md)
