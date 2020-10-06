## Console commands provided by this bundle

* `datalake:table:create [table identifier]` - Creates a metastore table based on it's YAML definition (name, schema, data path, ...)

* `datalake:table:delete [table identifier]` - Deletes a metastore table including data on HDFS

* `datalake:table:create-missing` - Creates newly defined tables that do not exist in the metastore yet

* `datalake:table:optimize-all` - Runs the OPTIMIZE command on all defined tables (Delta only)

___

Next section: [Defining DataLake tables](tables.md)
