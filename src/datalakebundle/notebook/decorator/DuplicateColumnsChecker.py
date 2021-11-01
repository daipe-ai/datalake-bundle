import collections
import pandas as pd
from logging import Logger
from typing import Iterable, Union
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator


class DuplicateColumnsChecker:
    def __init__(
        self,
        logger: Logger,
    ):
        self.__logger = logger

    def check(self, df: Union[DataFrame, pd.DataFrame], result_decorators: Iterable[DataFrameReturningDecorator]):
        duplicate_fields = [field_name for field_name, count in collections.Counter(self.__get_fields(df)).items() if count > 1]

        if not duplicate_fields:
            return

        fields2_tables = dict()

        for result_decorator in result_decorators:
            source_df = result_decorator.result
            for field_name in self.__get_fields(source_df):
                if field_name not in fields2_tables:
                    fields2_tables[field_name] = []

                fields2_tables[field_name].append(result_decorator.function.__name__)

        for duplicate_field in duplicate_fields:
            self.__logger.error(f"Duplicate field '{duplicate_field}'", extra={"source_functions": fields2_tables[duplicate_field]})

        fields_string = ", ".join(duplicate_fields)
        raise Exception(f"Duplicate output column(s): {fields_string}. Disable by setting @transformation(check_duplicate_columns=False)")

    def __get_fields(self, df: Union[DataFrame, pd.DataFrame]) -> Iterable[str]:
        if isinstance(df, DataFrame):
            return map(lambda field: field.name.lower(), df.schema.fields)
        else:
            return map(lambda field: field.lower(), df.columns)
