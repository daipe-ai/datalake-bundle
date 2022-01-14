from datalakebundle.filesystem.FilesystemInterface import FilesystemInterface


class FilesystemInjector:
    def __init__(self, filesystem: FilesystemInterface):
        self.__filesystem = filesystem

    def get(self):
        return self.__filesystem
