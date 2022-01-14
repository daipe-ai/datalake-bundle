from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from datalakebundle.write.PathWriterDecorator import PathWriterDecorator


@DecoratedDecorator
class delta_overwrite(PathWriterDecorator):  # noqa: N801
    _mode = "overwrite"
    _writer_service = "datalakebundle.delta.writer"
