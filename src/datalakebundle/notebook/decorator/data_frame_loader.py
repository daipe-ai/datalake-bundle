from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from injecta.container.ContainerInterface import ContainerInterface
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from datalakebundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface


@DecoratedDecorator  # pylint: disable = invalid-name
class data_frame_loader(DataFrameReturningDecorator):
    def __init__(self, *args, display=False):  # pylint: disable = super-init-not-called
        self._args = args
        self._display = display

    def after_execution(self, container: ContainerInterface):
        self._set_global_dataframe()

        if self._display and container.get_parameters().datalakebundle.notebook.display.enabled is True:
            data_frame_show_method: DataFrameShowMethodInterface = container.get("datalakebundle.dataframe.show_method")
            data_frame_show_method.show(self._result)
