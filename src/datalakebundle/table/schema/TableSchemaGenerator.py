from pyspark.sql import types as t
from typing import Union


class TableSchemaGenerator:
    def generate(self, schema: t.StructType) -> str:
        indent = "    "

        def generate_schema_recursively(element: Union[t.StructType, t.ArrayType, t.StructField], last_struct_type_indent: str = indent):
            schema_string = ""

            if isinstance(element, t.StructType):
                schema_string += last_struct_type_indent + "t.StructType(\n"
                schema_string += last_struct_type_indent + indent + "[\n"

                for field in element:
                    schema_string += generate_schema_recursively(field, last_struct_type_indent)

                schema_string += last_struct_type_indent + indent + "],\n"
                schema_string += last_struct_type_indent + "),\n"

            elif isinstance(element, t.ArrayType):
                if isinstance(element.elementType, t.StructType) or isinstance(element.elementType, t.ArrayType):
                    schema_string += last_struct_type_indent + "t.ArrayType(\n"
                    schema_string += generate_schema_recursively(element.elementType, last_struct_type_indent + indent)
                    schema_string += last_struct_type_indent + "),\n"

                else:
                    schema_string += last_struct_type_indent + f"t.ArrayType(t.{element.elementType}()),\n"

            elif isinstance(element, t.StructField):
                if isinstance(element.dataType, t.StructType):
                    schema_string += last_struct_type_indent + 2 * indent + "t.StructField(\n"
                    schema_string += last_struct_type_indent + 3 * indent + f'"{element.name}",\n'
                    schema_string += generate_schema_recursively(element.dataType, last_struct_type_indent + 3 * indent)
                    schema_string += last_struct_type_indent + 2 * indent + "),\n"

                elif isinstance(element.dataType, t.ArrayType):
                    if isinstance(element.dataType.elementType, t.StructType) or isinstance(element.dataType.elementType, t.ArrayType):
                        schema_string += last_struct_type_indent + 2 * indent + "t.StructField(\n"
                        schema_string += last_struct_type_indent + 3 * indent + f'"{element.name}",\n'
                        schema_string += generate_schema_recursively(element.dataType, last_struct_type_indent + 3 * indent)
                        schema_string += last_struct_type_indent + 2 * indent + "),\n"

                    else:
                        schema_string += (
                            last_struct_type_indent
                            + 2 * indent
                            + f't.StructField("{element.name}", t.ArrayType(t.{element.dataType.elementType}())),\n'
                        )

                else:
                    schema_string += last_struct_type_indent + 2 * indent + f't.StructField("{element.name}", t.{element.dataType}()),\n'

            return schema_string

        def remove_top_level_struct_type(schema_string):
            return "\n".join([line for line in schema_string.split("\n")[1:-2]]) + "\n"

        table_schema = ""

        table_schema += "def get_schema():\n"
        table_schema += "    return TableSchema(\n"

        schema_string = generate_schema_recursively(schema)
        schema_string = remove_top_level_struct_type(schema_string)
        table_schema += schema_string

        table_schema += '        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)\n'
        table_schema += '        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)\n'
        table_schema += "        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)\n"
        table_schema += "    )\n"

        return table_schema
