from daipecore.decorator.InputDecorator import InputDecorator


class DataFrameReturningDecorator(InputDecorator):
    def _set_global_dataframe(self):
        self._function.__globals__[self._function.__name__ + "_df"] = self._result
