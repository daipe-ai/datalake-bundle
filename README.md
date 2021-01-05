# Datalake bundle

![alt text](./docs/notebook-functions.png)

This bundle provides everything you need to create and manage a Databricks-based DataLake(House):

* Tools to simplify & automate table creation, updates and migrations.
* Explicit table schema enforcing for Hive tables, CSVs, ...
* Decorators to write well-maintainable and self-documented function-based notebooks
* Rich configuration options to customize naming standards, paths, and basically anything to match your needs

## Installation

Install the bundle via Poetry:

```
$ poetry add datalake-bundle
```

## Usage

1. [Recommended notebooks structure](docs/structure.md)
1. [Defining DataLake tables](docs/tables.md)
1. [Using datalake-specific notebook functions](docs/notebook-functions.md)
1. [Using table-specific configuration](docs/configuration.md)
1. [Tables management](docs/tables-management.md)
1. [Parsing fields from table identifier](docs/parsing-fields.md)
