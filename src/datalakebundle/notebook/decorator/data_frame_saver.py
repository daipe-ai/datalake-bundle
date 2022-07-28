# pylint: disable = super-init-not-called
from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator


@DecoratedDecorator
class data_frame_saver(InputDecorator):  # pylint: disable = invalid-name
    def __init__(self, *args):
        self._args = args
