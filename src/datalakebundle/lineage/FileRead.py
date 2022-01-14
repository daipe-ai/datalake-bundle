from daipecore.lineage.argument.DecoratorInputFunctionInterface import DecoratorInputFunctionInterface


class FileRead(DecoratorInputFunctionInterface):
    def __init__(self, path: str, type_: str):
        self.__path = path
        self.__type = type_

    @property
    def path(self):
        return self.__path

    @property
    def type(self):
        return self.__type

    @property
    def identifier(self):
        return self.__path
