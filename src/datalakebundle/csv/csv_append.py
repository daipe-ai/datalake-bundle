from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class csv_append(PathWriterDecorator):
    _mode = "append"
    _writer_service = "datalakebundle.csv.writer"
