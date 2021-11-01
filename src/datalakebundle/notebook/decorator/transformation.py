from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from pyspark.sql import DataFrame
import pandas as pd

from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from datalakebundle.notebook.decorator.DuplicateColumnsChecker import DuplicateColumnsChecker
from injecta.container.ContainerInterface import ContainerInterface
from typing import Union


@DecoratedDecorator
class transformation(DataFrameReturningDecorator):  # noqa: N801
    def __init__(self, *args, display=False, check_duplicate_columns=True):
        self._args = args
        self._display = display
        self._check_duplicate_columns = check_duplicate_columns

    def after_execution(self, container: ContainerInterface):
        if self._result is None:
            return

        if isinstance(self.result, DataFrame) or isinstance(self.result, pd.DataFrame):
            self.__process_df(container)
        else:
            raise TypeError(
                f"Result is of type: '{type(self._result).__name__}', only Spark and Pandas DataFrames are valid results in a @transformation."
            )

    def __process_df(self, container: ContainerInterface):
        if self._check_duplicate_columns and container.get_parameters().datalakebundle.notebook.duplicate_columns_check.enabled is True:
            duplicate_columns_checker: DuplicateColumnsChecker = container.get(DuplicateColumnsChecker)
            data_frame_decorators = filter(lambda decorator_arg: isinstance(decorator_arg, DataFrameReturningDecorator), self._args)
            duplicate_columns_checker.check(self._result, data_frame_decorators)

        if self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            self.__display(container, self.__get_display_service(self._result))

        self._set_global_dataframe()

    def __display(self, container: ContainerInterface, show_method: str):
        show_service = container.get(show_method)
        show_service.show(self._result)

    def __get_display_service(self, df: Union[DataFrame, pd.DataFrame]) -> str:
        if isinstance(df, DataFrame):
            return "pysparkbundle.dataframe.show_method"
        return "daipecore.pandas.dataframe.show_method"
