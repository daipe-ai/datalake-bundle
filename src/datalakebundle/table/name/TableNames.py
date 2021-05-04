class TableNames:
    def __init__(self, db_identifier: str, db_name: str, table_identifier: str, table_name: str):
        self.__db_identifier = db_identifier
        self.__db_name = db_name
        self.__table_identifier = table_identifier
        self.__table_name = table_name

    @property
    def db_identifier(self):
        return self.__db_identifier

    @property
    def db_name(self):
        return self.__db_name

    @property
    def table_identifier(self):
        return self.__table_identifier

    @property
    def table_name(self):
        return self.__table_name

    @property
    def identifier(self):
        return self.__db_identifier + "." + self.__table_identifier

    @property
    def full_table_name(self):
        return self.__db_name + "." + self.__table_name

    def to_dict(self):
        return dict(
            identifier=self.identifier,
            db_identifier=self.__db_identifier,
            db_name=self.__db_name,
            table_identifier=self.__table_identifier,
            table_name=self.__table_name,
        )
