## Parsing fields from table identifier

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

___

Next section: [Console commands provided by this bundle](console-commands.md)
