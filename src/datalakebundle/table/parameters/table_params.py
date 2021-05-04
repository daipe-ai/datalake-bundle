from daipecore.decorator.StringableParameterInterface import StringableParameterInterface


class table_params(StringableParameterInterface):  # noqa: N801
    def __init__(self, identifier: str, param_path_parts: list = None):
        self._identifier = identifier
        self._param_path_parts = param_path_parts if param_path_parts else []

    def to_string(self):
        base_path = f'datalakebundle.tables."{self._identifier}".params'

        if self._param_path_parts:
            return "%" + base_path + "." + ".".join(self._param_path_parts) + "%"

        return "%" + base_path + "%"

    def __getattr__(self, item):
        param_path_parts = self._param_path_parts.copy()
        param_path_parts.append(item)

        return table_params(self._identifier, param_path_parts)
