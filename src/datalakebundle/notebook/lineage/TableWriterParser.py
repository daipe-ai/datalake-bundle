import _ast
from daipecore.lineage.DecoratorParserInterface import DecoratorParserInterface
from datalakebundle.notebook.lineage.TableWriter import TableWriter


class TableWriterParser(DecoratorParserInterface):
    def __init__(self, name: str, mode: str):
        self.__name = name
        self.__mode = mode

    def parse(self, decorator: _ast.Call):
        arg: _ast.Str = decorator.args[0]

        return TableWriter(arg.s, self.__mode)

    def get_name(self) -> str:
        return self.__name
