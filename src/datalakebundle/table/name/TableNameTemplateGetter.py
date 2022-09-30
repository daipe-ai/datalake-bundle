class TableNameTemplateGetter:
    def __init__(self, table_name_read_template: str, table_name_write_template: str):
        self.__table_name_read_template = table_name_read_template
        self.__table_name_write_template = table_name_write_template

    def get_template_for_read(self) -> str:
        return self.__table_name_read_template

    def get_template_for_write(self) -> str:
        return self.__table_name_write_template
