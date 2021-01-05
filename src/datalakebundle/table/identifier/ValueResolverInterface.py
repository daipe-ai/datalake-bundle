from abc import ABC, abstractmethod
from box import Box

class ValueResolverInterface(ABC):

    @abstractmethod
    def resolve(self, rawTableConfig: Box):
        pass

    def getDependingFields(self) -> set:
        return set()
