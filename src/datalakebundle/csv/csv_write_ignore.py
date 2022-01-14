from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator
class csv_write_ignore(PathWriterDecorator):  # noqa: N801
    _mode = "ignore"
    _writer_service = "datalakebundle.csv.writer"
