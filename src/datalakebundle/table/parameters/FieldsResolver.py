from box import Box
from datalakebundle.table.parameters.ValueResolverFactory import ValueResolverFactory


class FieldsResolver:
    def __init__(self):
        self.__value_resolver_factory = ValueResolverFactory()

    def resolve(self, all_fields: dict, defaults: dict):
        resolver_keys = [name for name, val in defaults.items() if isinstance(val, dict) and name not in all_fields]

        unsuccessfully_assigned_fields = set()

        while resolver_keys:
            name = resolver_keys.pop(0)
            resolver = self.__value_resolver_factory.create(defaults[name])
            dependent_fields = resolver.get_depending_fields()

            if dependent_fields - set(all_fields.keys()) == set():
                unsuccessfully_assigned_fields = set()
                all_fields[name] = resolver.resolve(Box(all_fields))
            else:
                if name in unsuccessfully_assigned_fields:
                    raise Exception(
                        f'Infinite assignment loop detected. Check get_depending_fields() of {defaults[name]["resolver_class"]}'
                    )

                unsuccessfully_assigned_fields.add(name)
                resolver_keys.append(name)

        return all_fields
