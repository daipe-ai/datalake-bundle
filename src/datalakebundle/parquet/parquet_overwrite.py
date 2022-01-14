from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class parquet_overwrite(PathWriterDecorator):
    _mode = "overwrite"
    _writer_service = "datalakebundle.parquet.writer"
