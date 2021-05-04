from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from daipecore.decorator.OutputDecorator import OutputDecorator
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame
from datalakebundle.table.create.TableDefinitionFactory import TableDefinitionFactory
from datalakebundle.table.schema.TableSchema import TableSchema
from datalakebundle.table.write.TableUpserter import TableUpserter


@DecoratedDecorator
class table_upsert(OutputDecorator):  # noqa: N801
    def __init__(self, identifier: str, table_schema: TableSchema):
        self.__identifier = identifier
        self.__table_schema = table_schema

    def process_result(self, result: DataFrame, container: ContainerInterface):
        table_definition_factory: TableDefinitionFactory = container.get(TableDefinitionFactory)
        table_upserter: TableUpserter = container.get(TableUpserter)

        if not isinstance(self.__table_schema, TableSchema):
            raise Exception(f"Invalid table schema: {self.__table_schema}")

        if not self.__table_schema.primary_key:
            raise Exception("Table schema must have primary key defined for upsert to work properly")

        table_definition = table_definition_factory.create_from_table_schema(self.__identifier, self.__table_schema)

        table_upserter.upsert(result, table_definition)
