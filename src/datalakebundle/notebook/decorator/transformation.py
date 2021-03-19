# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as display_function
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from datalakebundle.notebook.decorator.DuplicateColumnsChecker import DuplicateColumnsChecker


class transformation(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):  # noqa: N801
    def __init__(self, *args, display=False, check_duplicate_columns=True):  # pylint: disable = unused-argument
        self._display = display
        self._check_duplicate_columns = check_duplicate_columns

    def after_execution(self, container: ContainerInterface):
        if self._check_duplicate_columns and container.get_parameters().datalakebundle.notebook.duplicate_columns_check.enabled is True:
            duplicate_columns_checker: DuplicateColumnsChecker = container.get(DuplicateColumnsChecker)

            data_frame_decorators = tuple(
                decorator_arg for decorator_arg in self._decorator_args if isinstance(decorator_arg, DataFrameReturningDecorator)
            )
            duplicate_columns_checker.check(self._result, data_frame_decorators)

        if self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            display_function(self._result)
