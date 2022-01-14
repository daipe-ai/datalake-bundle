from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class json_write_ignore(PathWriterDecorator):
    _mode = "ignore"
    _writer_service = "datalakebundle.json.writer"
