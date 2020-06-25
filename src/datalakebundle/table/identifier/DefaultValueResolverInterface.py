from abc import ABC, abstractmethod
from box import Box

class DefaultValueResolverInterface(ABC):

    @abstractmethod
    def resolve(self, rawTableConfig: Box):
        pass
