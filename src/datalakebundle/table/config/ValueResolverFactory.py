from injecta.module import attribute_loader
from injecta.service.parser.DTypeResolver import DTypeResolver
from datalakebundle.table.identifier.ValueResolverInterface import ValueResolverInterface


class ValueResolverFactory:
    def __init__(self):
        self.__d_type_resolver = DTypeResolver()

    def create(self, val) -> ValueResolverInterface:
        resolver_type = self.__d_type_resolver.resolve(val["resolver_class"])
        resolver_class = attribute_loader.load(resolver_type.module_name, resolver_type.class_name)
        resolver_arguments = val["resolver_arguments"] if "resolver_arguments" in val else []

        return resolver_class(*resolver_arguments)
