import json


class TableParameters:
    def __init__(
        self,
        db_identifier: str,
        db_name: str,
        table_identifier: str,
        table_name: str,
        target_path: str,
        **kwargs,
    ):
        self.__db_identifier = db_identifier
        self.__db_name = db_name
        self.__table_identifier = table_identifier
        self.__table_name = table_name
        self.__target_path = target_path
        self.__custom_fields = kwargs

    @property
    def identifier(self):
        return self.__db_identifier + "." + self.__table_identifier

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
    def target_path(self):
        return self.__target_path

    @property
    def full_table_name(self):
        return self.__db_name + "." + self.__table_name

    def get_custom_fields(self):
        return self.__custom_fields

    def to_dict(self):
        return {
            **self.__custom_fields,
            **{
                "identifier": self.identifier,
                "db_identifier": self.__db_identifier,
                "db_name": self.__db_name,
                "table_identifier": self.__table_identifier,
                "table_name": self.__table_name,
                "target_path": self.__target_path,
            },
        }

    def __getattr__(self, item):
        if item not in self.__custom_fields:
            raise Exception(f'Unexpected attribute: "{item}"')

        return self.__custom_fields[item]

    def __eq__(self, other: "TableParameters"):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4)
