from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class delta_append(PathWriterDecorator):
    _mode = "append"
    _writer_service = "pysparkbundle.delta.writer"
