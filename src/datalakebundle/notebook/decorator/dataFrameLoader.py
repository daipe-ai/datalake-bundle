# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator

class dataFrameLoader(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, display=False): # pylint: disable = unused-argument
        self._display = display

    def afterExecution(self, container: ContainerInterface):
        if self._display and container.getParameters().datalakebundle.notebook.display.enabled is True:
            displayFunction(self._result)
