from abc import ABC, abstractmethod

class IdentifierParserInterface(ABC):

    @abstractmethod
    def parse(self, identifier: str) -> dict:
        pass
