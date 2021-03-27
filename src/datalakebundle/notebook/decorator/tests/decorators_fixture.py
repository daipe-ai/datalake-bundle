import os
from datalakebundle.notebook.decorator.data_frame_loader import data_frame_loader
from datalakebundle.notebook.decorator.transformation import transformation
from datalakebundle.notebook.decorator.data_frame_saver import data_frame_saver

os.environ["APP_ENV"] = "test"


@data_frame_loader()
def load_data():
    return 155


@data_frame_loader()
def load_data2():
    return 145


@transformation(load_data, load_data2, check_duplicate_columns=False)
def sumup(police_number: int, something: int):
    assert police_number == 155
    assert something == 145

    return 155 + 145


@data_frame_loader
def load_data3():
    return 111


@data_frame_saver(sumup)
def save(result: int):
    assert result == 300
