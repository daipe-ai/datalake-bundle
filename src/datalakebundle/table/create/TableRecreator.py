from logging import Logger
from datalakebundle.delta.DeltaStorage import DeltaStorage
from datalakebundle.table.create.TableDefinition import TableDefinition
from datalakebundle.table.write.TablePropertiesSetter import TablePropertiesSetter


class TableRecreator:
    def __init__(
        self,
        logger: Logger,
        delta_storage: DeltaStorage,
        table_properties_setter: TablePropertiesSetter,
    ):
        self.__logger = logger
        self.__delta_storage = delta_storage
        self.__table_properties_setter = table_properties_setter

    def recreate(self, table_definition: TableDefinition):
        self.__logger.info(f"Recreating table {table_definition.full_table_name}")

        self.__delta_storage.recreate_table(table_definition)

        self.__table_properties_setter.set(table_definition)

        self.__logger.info(f"Table {table_definition.full_table_name} successfully recreated")
