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

1. [Defining DataLake tables](docs/tables.md)
1. [Parsing fields from table identifier](docs/parsing-fields.md)
1. [Console commands](docs/console-commands.md)
