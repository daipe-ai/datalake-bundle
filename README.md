# Datalake bundle

Table & schema management for your Databricks-based data lake (house).

Provides console commands to simplify table creation, update/migration and deletion.

## Installation

Install the bundle via Poetry:

```
$ poetry add datalake-bundle
```

Add the bundle to your application's kernel to activate it:

```python
from pyfony.kernel.BaseKernel import BaseKernel
from datalakebundle.DataLakeBundle import DataLakeBundle

class Kernel(BaseKernel):
    
    def _registerBundles(self):
        return [
            # ...
            DataLakeBundle(),
            # ...
        ]
```

## Usage

### Defining tables

Add the following configuration to your app:

```yaml
parameters:
  datalakebundle:
    tables:
      customer.my_table:
        schemaPath: 'myapp.client.schema'
        targetPath: '/data/clients.delta'
      product.another_table:
        schemaPath: 'myapp.product.schema'
        targetPath: '/data/products.delta'
        partitionBy: ['date'] # optional table partitioning customization
```

### Customizing table names

Table naming can be customized to match your company naming conventions. 

By default, all tables are prefixed with the environment name (dev/test/prod/...):

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '%kernel.environment%_{identifier}'
```

The `{identifier}` is resolved to the table identifier defined in the `datalakebundle.tables` configuration (see above).

By changing the `nameTemplate` option, you may add some prefix or suffix to both the database or the table names:

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '%kernel.environment%_{dbIdentifier}.tableprefix_{tableIdentifier}_tablesufix'
```

### Parsing fields from table identifier:

Sometimes your table names may contain additional flags to explicitly emphasize some meta-information about the data stored in that particular table.

Imagine that you have the following tables:

* `customer_e.my_table`
* `product_p.another_table`

The `e/p` suffixes describe the fact that the table contains *encrypted* or *plain* data. What if we need to use that information in our code?

You may always define the attribute manually in the tables configuration: 

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '{identifier}'
    tables:
      customer_e.my_table:
        schemaPath: 'datalakebundle.test.TestSchema'
        encrypted: True
      product_p.another_table:
        schemaPath: 'datalakebundle.test.AnotherSchema'
        encrypted: False
```

If you don't want to duplicate the configuration, try using the `defaults` config option to parse the *encrypted/plain* flag into the new `encrypted` boolean table attribute: 

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '{identifier}'
      defaults:
        encrypted: !expr 'dbIdentifier[-1:] == "e"'
    tables:
      customer_e.my_table:
        schemaPath: 'datalakebundle.test.TestSchema'
      product_p.another_table:
        schemaPath: 'datalakebundle.test.AnotherSchema'
```

For more complex cases, you may also use a custom resolver to create a new table attribute:

```python
from box import Box
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface

class TargetPathResolver(ValueResolverInterface):

    def __init__(self, basePath: str):
        self.__basePath = basePath

    def resolve(self, rawTableConfig: Box):
        encryptedString = 'encrypted' if rawTableConfig.encrypted is True else 'plain'

        return self.__basePath + '/' + rawTableConfig.dbIdentifier + '/' + encryptedString + '/' + rawTableConfig.tableIdentifier + '.delta'
```

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '{identifier}'
      defaults:
        targetPath:
          resolverClass: 'datalakebundle.test.TargetPathResolver'
          resolverArguments:
            - '%datalake.basePath%'
    tables:
      customer_e.my_table:
        schemaPath: 'datalakebundle.test.TestSchema'
      product_p.another_table:
        schemaPath: 'datalakebundle.test.AnotherSchema'
```

### Console commands provided by this bundle

* `datalake:table:create [table identifier]` - Creates a metastore table based on it's YAML definition (name, schema, data path, ...)

* `datalake:table:delete [table identifier]` - Deletes a metastore table including data on HDFS

* `datalake:table:create-missing` - Creates newly defined tables that do not exist in the metastore yet

* `datalake:table:optimize-all` - Runs the OPTIMIZE command on all defined tables (Delta only)
