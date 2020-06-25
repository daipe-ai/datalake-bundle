import importlib

class SchemaGetter:

    def get(self, schemaPath: str):
        getSchema = getattr(importlib.import_module(schemaPath), 'getSchema')

        return getSchema()
