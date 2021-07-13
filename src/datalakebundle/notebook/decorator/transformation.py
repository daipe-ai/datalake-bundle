from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from pyspark.sql import DataFrame

from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from datalakebundle.notebook.decorator.DuplicateColumnsChecker import DuplicateColumnsChecker
from injecta.container.ContainerInterface import ContainerInterface
from pysparkbundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface


@DecoratedDecorator
class transformation(DataFrameReturningDecorator):  # noqa: N801
    def __init__(self, *args, display=False, check_duplicate_columns=True):
        self._args = args
        self._display = display
        self._check_duplicate_columns = check_duplicate_columns

    def after_execution(self, container: ContainerInterface):
        if (
            self._result is not None
            and isinstance(self.result, DataFrame)
            and self._check_duplicate_columns
            and container.get_parameters().datalakebundle.notebook.duplicate_columns_check.enabled is True
        ):
            duplicate_columns_checker: DuplicateColumnsChecker = container.get(DuplicateColumnsChecker)

            data_frame_decorators = tuple(
                decorator_arg for decorator_arg in self._args if isinstance(decorator_arg, DataFrameReturningDecorator)
            )
            duplicate_columns_checker.check(self._result, data_frame_decorators)

        self._set_global_dataframe()

        if self._result is not None and self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            data_frame_show_method: DataFrameShowMethodInterface = container.get("pysparkbundle.dataframe.show_method")
            data_frame_show_method.show(self._result)
