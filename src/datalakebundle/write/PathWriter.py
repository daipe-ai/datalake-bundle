from typing import Optional
from logging import Logger
from pyspark.sql import DataFrame


class PathWriter:
    def __init__(self, format_: str, logger: Logger):
        self.__format = format_
        self.__logger = logger

    def write(self, df: DataFrame, path: str, mode: str, partition_by: Optional[list] = None, options: Optional[dict] = None):
        self.__logger.info(
            f"Writing data to: {path}",
            extra={
                "mode": mode,
                "partition_by": partition_by,
                "options": options or {},
            },
        )

        data_frame_writer = df.write.mode(mode)

        if partition_by:
            data_frame_writer = data_frame_writer.partitionBy(partition_by)

        if options:
            data_frame_writer = data_frame_writer.options(**options)

        data_frame_writer.format(self.__format).save(path)

        self.__logger.info(f"Data successfully written to: {path}")
