from daipecore.lineage.argument.DecoratorInputFunctionInterface import DecoratorInputFunctionInterface


class ReadTable(DecoratorInputFunctionInterface):
    def __init__(self, full_table_name: str):
        self.__full_table_name = full_table_name

    @property
    def full_table_name(self):
        return self.__full_table_name

    @property
    def identifier(self):
        return self.__full_table_name
