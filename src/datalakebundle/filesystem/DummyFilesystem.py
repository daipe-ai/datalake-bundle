from datalakebundle.filesystem.FilesystemInterface import FilesystemInterface


class DummyFilesystem(FilesystemInterface):
    def exists(self, path: str):
        pass

    def put(self, path: str, content: str, overwrite: bool = False):
        pass

    def makedirs(self, path: str):
        pass

    def copy(self, source: str, destination: str, recursive: bool = False):
        pass

    def move(self, source: str, destination: str, recursive: bool = False):
        pass

    def delete(self, path: str, recursive: bool = False):
        pass
