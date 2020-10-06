## Defining DataLake tables

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

___

Next section: [Parsing fields from table identifier](parsing-fields.md)
