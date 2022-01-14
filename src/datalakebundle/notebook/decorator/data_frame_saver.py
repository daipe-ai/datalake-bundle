# pylint: disable = super-init-not-called
from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator


@DecoratedDecorator  # pylint: disable = invalid-name
class data_frame_saver(InputDecorator):
    def __init__(self, *args):
        self._args = args
