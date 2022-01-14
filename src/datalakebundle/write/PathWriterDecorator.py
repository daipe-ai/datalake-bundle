from typing import Optional, Union
from datalakebundle.write.PathWriter import PathWriter
from daipecore.decorator.OutputDecorator import OutputDecorator
from daipecore.function import arguments_transformer
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame


class PathWriterDecorator(OutputDecorator):  # pyre-ignore[13]

    _mode: str
    _writer_service: str

    def __init__(self, path: str, partition_by: Optional[Union[str, list]] = None, options: Optional[dict] = None):
        self.__path = path

        if partition_by is None:
            self.__partition_by = []
        elif isinstance(partition_by, str):
            self.__partition_by = [partition_by]
        elif isinstance(partition_by, list):
            self.__partition_by = partition_by
        else:
            raise Exception(f"Unexpected partition_by type: {type(partition_by)}")

        self.__options = options

    def process_result(self, result: DataFrame, container: ContainerInterface):
        transformed_path = arguments_transformer.transform(self.__path, container)

        path_writer: PathWriter = container.get(self._writer_service)
        path_writer.write(result, transformed_path, self._mode, self.__partition_by, self.__options)
