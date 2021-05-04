import types
from injecta.module import attribute_loader


def load(table_schema_path: str):
    last_dot_pos = table_schema_path.rfind(".")

    if not last_dot_pos:
        raise Exception(f"Invalid class path: {table_schema_path}")

    module_name = table_schema_path[:last_dot_pos]
    attribute_name = table_schema_path[last_dot_pos + 1 :]  # noqa: E203

    schema = attribute_loader.load(module_name, attribute_name)

    if isinstance(schema, types.FunctionType):
        return schema()
    else:
        return schema
