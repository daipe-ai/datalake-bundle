# pylint: disable = invalid-name, not-callable
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass


class data_frame_saver(AbstractDecorator, metaclass=DecoratorMetaclass):  # noqa: N801

    # empty __init__() to suppress PyCharm's "unexpected arguments" error
    def __init__(self, *args):
        pass
