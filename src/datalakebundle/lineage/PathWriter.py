from daipecore.lineage.OutputDecoratorInterface import OutputDecoratorInterface


class PathWriter(OutputDecoratorInterface):
    def __init__(self, path: str, mode: str):
        self.__path = path
        self.__mode = mode

    @property
    def path(self):
        return self.__path

    @property
    def mode(self):
        return self.__mode

    @property
    def identifier(self):
        return self.__path
