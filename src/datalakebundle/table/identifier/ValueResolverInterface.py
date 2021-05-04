from abc import ABC, abstractmethod
from box import Box


class ValueResolverInterface(ABC):
    @abstractmethod
    def resolve(self, raw_table_parameters: Box):
        pass

    def get_depending_fields(self) -> set:
        return set()
