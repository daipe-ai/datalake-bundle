from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator
class parquet_write_errorifexists(PathWriterDecorator):  # noqa: N801
    _mode = "errorifexists"
    _writer_service = "datalakebundle.parquet.writer"
