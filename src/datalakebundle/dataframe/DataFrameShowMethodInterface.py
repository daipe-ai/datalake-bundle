from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class DataFrameShowMethodInterface(ABC):
    @abstractmethod
    def show(self, df: DataFrame):
        pass
