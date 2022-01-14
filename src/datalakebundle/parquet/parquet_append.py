from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator
class parquet_append(PathWriterDecorator):  # noqa: N801
    _mode = "append"
    _writer_service = "datalakebundle.parquet.writer"
