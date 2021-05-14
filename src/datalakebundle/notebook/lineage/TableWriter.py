from daipecore.lineage.OutputDecoratorInterface import OutputDecoratorInterface


class TableWriter(OutputDecoratorInterface):
    def __init__(self, full_table_name: str, mode: str):
        self.__full_table_name = full_table_name
        self.__mode = mode

    @property
    def full_table_name(self):
        return self.__full_table_name

    @property
    def mode(self):
        return self.__mode

    @property
    def identifier(self):
        return self.__full_table_name
