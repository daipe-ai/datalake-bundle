from logging import Logger
from datalakebundle.delta.DeltaStorage import DeltaStorage
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.write.TablePropertiesSetter import TablePropertiesSetter


class TableCreator:
    def __init__(
        self,
        logger: Logger,
        delta_storage: DeltaStorage,
        table_existence_checker: TableExistenceChecker,
        table_properties_setter: TablePropertiesSetter,
    ):
        self.__logger = logger
        self.__delta_storage = delta_storage
        self.__table_existence_checker = table_existence_checker
        self.__table_properties_setter = table_properties_setter

    def create(self, table_definition: TableDefinition):
        self.__logger.info(f"Creating new table {table_definition.full_table_name} for {table_definition.target_path}")

        self.__delta_storage.create_table(table_definition)

        self.__table_properties_setter.set(table_definition)

        self.__logger.info(f"Table {table_definition.full_table_name} successfully created")

    def create_if_not_exists(self, table_definition: TableDefinition):
        if self.__table_existence_checker.table_exists(table_definition.db_name, table_definition.table_name):
            self.__logger.info(f"Table {table_definition.full_table_name} already exists, creation skipped")
            return

        self.create(table_definition)
