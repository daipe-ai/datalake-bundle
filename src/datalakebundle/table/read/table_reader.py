from logging import Logger
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import SparkSession
from datalakebundle.table.parameters.TableParametersManager import TableParametersManager
from datalakebundle.table.name.TableNameTemplateGetter import TableNameTemplateGetter


@input_decorator_function
def read_table(identifier: str):
    def wrapper(container: ContainerInterface):
        table_name_template_getter: TableNameTemplateGetter = container.get(TableNameTemplateGetter)
        table_parameters_manager: TableParametersManager = container.get(TableParametersManager)
        table_name_template = table_name_template_getter.get_template_for_read()
        table_parameters = table_parameters_manager.get_or_parse(table_name_template, identifier)

        logger: Logger = container.get("datalakebundle.logger")
        logger.info(f"Reading table `{table_parameters.full_table_name}`")

        spark: SparkSession = container.get(SparkSession)

        return spark.read.table(table_parameters.full_table_name)

    return wrapper
