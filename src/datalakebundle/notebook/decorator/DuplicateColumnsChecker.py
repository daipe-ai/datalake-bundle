import collections
from logging import Logger
from typing import Tuple
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator


class DuplicateColumnsChecker:
    def __init__(
        self,
        logger: Logger,
    ):
        self.__logger = logger

    def check(self, df: DataFrame, result_decorators: Tuple[DataFrameReturningDecorator]):
        field_names = [field.name.lower() for field in df.schema.fields]
        duplicate_fields = dict()

        for field_name, count in collections.Counter(field_names).items():
            if count > 1:
                duplicate_fields[field_name] = []

        if duplicate_fields == dict():
            return

        fields2_tables = dict()

        for result_decorator in result_decorators:
            source_df = result_decorator.result
            for field in source_df.schema.fields:
                field_name = field.name.lower()

                if field_name not in fields2_tables:
                    fields2_tables[field_name] = []

                fields2_tables[field_name].append(result_decorator.function.__name__)

        for duplicate_field in duplicate_fields:
            self.__logger.error(f"Duplicate field {duplicate_field}", extra={"source_dataframes": fields2_tables[duplicate_field]})

        fields_string = ", ".join(duplicate_fields)
        raise Exception(f"Duplicate output column(s): {fields_string}. Disable by setting @transformation(check_duplicate_columns=False)")
