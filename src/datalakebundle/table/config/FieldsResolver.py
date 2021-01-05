from box import Box
from datalakebundle.table.config.ValueResolverFactory import ValueResolverFactory

class FieldsResolver:

    def __init__(self):
        self.__valueResolverFactory = ValueResolverFactory()

    def resolve(self, allFields: dict, defaults: dict):
        resolverKeys = [name for name, val in defaults.items() if isinstance(val, dict) and name not in allFields]

        while resolverKeys:
            name = resolverKeys.pop(0)
            resolver = self.__valueResolverFactory.create(defaults[name])
            dependentFields = resolver.getDependingFields()

            if dependentFields - set(allFields.keys()) == set():
                allFields[name] = resolver.resolve(Box(allFields))
            else:
                resolverKeys.append(name)

        return allFields
