from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class json_append(PathWriterDecorator):
    _mode = "append"
    _writer_service = "datalakebundle.json.writer"
