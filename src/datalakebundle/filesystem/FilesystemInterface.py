from abc import ABC, abstractmethod


class FilesystemInterface(ABC):
    @abstractmethod
    def exists(self, path: str):
        pass

    @abstractmethod
    def put(self, path: str, content: str, overwrite: bool = False):
        pass

    @abstractmethod
    def makedirs(self, path: str):
        pass

    @abstractmethod
    def copy(self, source: str, destination: str, recursive: bool = False):
        pass

    @abstractmethod
    def move(self, source: str, destination: str, recursive: bool = False):
        pass

    @abstractmethod
    def delete(self, path: str, recursive: bool = False):
        pass
