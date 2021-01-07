import pyspark.sql.types as T
import importlib

class SchemaGetter:

    def get(self, schemaPath: str) -> T.StructType:
        getSchema = getattr(importlib.import_module(schemaPath), 'getSchema')

        return getSchema()
