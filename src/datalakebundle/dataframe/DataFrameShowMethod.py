from pyspark.sql import DataFrame
from datalakebundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface


class DataFrameShowMethod(DataFrameShowMethodInterface):
    def show(self, df: DataFrame):
        df.show()
