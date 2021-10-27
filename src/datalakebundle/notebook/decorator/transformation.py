from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from logging import Logger
from pyspark.sql import DataFrame
import pandas as pd

from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from datalakebundle.notebook.decorator.DuplicateColumnsChecker import DuplicateColumnsChecker
from injecta.container.ContainerInterface import ContainerInterface
from typing import Tuple, cast


@DecoratedDecorator
class transformation(DataFrameReturningDecorator):  # noqa: N801
    def __init__(self, *args, display=False, check_duplicate_columns=True):
        self._args = args
        self._display = display
        self._check_duplicate_columns = check_duplicate_columns

    def after_execution(self, container: ContainerInterface):
        if self._result is None:
            logger: Logger = container.get("datalakebundle.logger")
            logger.warning("Result Dataframe is empty.")
            return

        if isinstance(self.result, DataFrame):
            self.__spark_df(container)
        elif isinstance(self.result, pd.DataFrame):
            self.__pandas_df(container)
        else:
            raise TypeError(
                f"Result is of type: '{type(self._result)}', only Spark and Pandas DataFrames are valid results in @transformation."
            )

        self._set_global_dataframe()

    def __spark_df(self, container: ContainerInterface):
        duplicate_columns_checker: DuplicateColumnsChecker = container.get(DuplicateColumnsChecker)
        data_frame_decorators = cast(
            Tuple[DataFrameReturningDecorator],
            tuple(filter(lambda decorator_arg: isinstance(decorator_arg, DataFrameReturningDecorator), self._args)),
        )
        duplicate_columns_checker.check(self._result, data_frame_decorators)

        self.__display(container, "pysparkbundle.dataframe.show_method")

    def __pandas_df(self, container: ContainerInterface):
        self.__display(container, "daipecore.pandas.dataframe.show_method")

    def __display(self, container: ContainerInterface, show_method: str):
        if self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            show_service = container.get(show_method)
            show_service.show(self._result)
