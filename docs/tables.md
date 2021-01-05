## Defining DataLake tables

To define the first tables, add the following configuration to your `config.yaml`:

```yaml
parameters:
  datalakebundle:
    tables:
      customer.my_table:
      product.another_table:
        partitionBy: ['date'] # optional table partitioning customization
```

### (required) Setting datalake storage path

Add the following configuration to `config.yaml` to set the default storage path for all the datalake tables:

```yaml
parameters:
  datalakebundle:
    defaults:
      targetPath: '/mybase/data/{dbIdentifier}/{tableIdentifier}.delta'
```

When setting `defaults`, you can utilize the following placeholders:

* `{identifier}` - `customer.my_table`
* `{dbIdentifier}` - `customer`
* `{tableIdentifier}` - `my_table`
* [parsed custom fields](parsing-fields.md)

To modify storage path of any specific table, add the `targetPath` attribute to given table's configuration:

```yaml
parameters:
  datalakebundle:
    tables:
      customer.my_table:
        targetPath: '/some_custom_base/{dbIdentifier}/{tableIdentifier}.delta'
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

By changing the `nameTemplate` option, you may add some prefix or suffix to both the database, or the table names:

```yaml
parameters:
  datalakebundle:
    table:
      nameTemplate: '%kernel.environment%_{dbIdentifier}.tableprefix_{tableIdentifier}_tablesufix'
```

___

Next section: [Using datalake-specific notebook functions](notebook-functions.md)
