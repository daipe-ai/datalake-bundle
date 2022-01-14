from typing import Optional
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql.types import StructType
from daipecore.function.input_decorator_function import input_decorator_function
from datalakebundle.read.PathReader import PathReader


@input_decorator_function
def read_delta(path: str, schema: Optional[StructType] = None, options: Optional[dict] = None):
    def wrapper(container: ContainerInterface):
        path_reader: PathReader = container.get("datalakebundle.delta.reader")

        return path_reader.read(path, schema, options)

    return wrapper
