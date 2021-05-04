from pyspark.sql.types import StructType


class UpsertQueryCreator:
    def create(self, full_table_name: str, schema: StructType, primary_key: list, temp_source_table: str) -> str:
        conditions = []
        updates = []
        columns_to_update = set(schema.fieldNames()) - set(primary_key)

        for primary_key_column in primary_key:
            conditions.append(f"source.`{primary_key_column}` = target.`{primary_key_column}`")

        for col in columns_to_update:
            updates.append(f"target.`{col}` = source.`{col}`")

        query = (
            f"MERGE INTO {full_table_name} AS target\n"
            f"USING {temp_source_table} AS source\n"
            f"ON {' AND '.join(conditions)}\n"
            f"{{matched_clause}}"
            f"WHEN NOT MATCHED THEN INSERT *\n"
        )

        if len(updates) > 0:
            query = query.format(matched_clause=f"WHEN MATCHED THEN UPDATE SET {', '.join(updates)}\n")
        else:
            query = query.format(matched_clause="")

        return query
