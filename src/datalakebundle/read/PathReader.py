from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class PathReader:
    def __init__(self, format_: str, logger: Logger, spark: SparkSession):
        self.__format = format_
        self.__logger = logger
        self.__spark = spark

    def read(self, path: str, schema: StructType = None, options: dict = None):
        self.__logger.info(f"Reading {self.__format} from `{path}`", extra={"options": options})

        data_frame_reader = self.__spark.read.format(self.__format)

        if schema:
            data_frame_reader = data_frame_reader.schema(schema)

        if options:
            data_frame_reader = data_frame_reader.options(**options)

        return data_frame_reader.load(path)
