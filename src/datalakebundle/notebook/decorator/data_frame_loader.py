# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as display_function
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator


class data_frame_loader(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):  # noqa: N801
    def __init__(self, *args, display=False):  # pylint: disable = unused-argument
        self._display = display

    def after_execution(self, container: ContainerInterface):
        if self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            display_function(self._result)
