from box import Box
from datalakebundle.table.config.ValueResolverFactory import ValueResolverFactory

class FieldsResolver:

    def __init__(self):
        self.__valueResolverFactory = ValueResolverFactory()

    def resolve(self, allFields: dict, defaults: dict):
        resolverKeys = [name for name, val in defaults.items() if isinstance(val, dict) and name not in allFields]

        unsuccessfullyAssignedFields = set()

        while resolverKeys:
            name = resolverKeys.pop(0)
            resolver = self.__valueResolverFactory.create(defaults[name])
            dependentFields = resolver.getDependingFields()

            if dependentFields - set(allFields.keys()) == set():
                unsuccessfullyAssignedFields = set()
                allFields[name] = resolver.resolve(Box(allFields))
            else:
                if name in unsuccessfullyAssignedFields:
                    raise Exception(f'Infinite assignment loop detected. Check getDependingFields() of {defaults[name]["resolverClass"]}')

                unsuccessfullyAssignedFields.add(name)
                resolverKeys.append(name)

        return allFields
