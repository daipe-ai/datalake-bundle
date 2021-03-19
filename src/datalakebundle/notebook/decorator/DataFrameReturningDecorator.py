from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator


class DataFrameReturningDecorator(AbstractDecorator):
    def on_execution(self, container: ContainerInterface):
        result = super().on_execution(container)
        self._function.__globals__[self._function.__name__ + "_df"] = result

        return result
