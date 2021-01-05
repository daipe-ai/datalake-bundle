# pylint: disable = all
import os
from datalakebundle.notebook.decorator.dataFrameLoader import dataFrameLoader
from datalakebundle.notebook.decorator.transformation import transformation
from datalakebundle.notebook.decorator.dataFrameSaver import dataFrameSaver

os.environ['APP_ENV'] = 'test'

@dataFrameLoader()
def load_data():
    return 155

@dataFrameLoader()
def load_data2():
    return 145

@transformation(load_data, load_data2, checkDuplicateColumns=False)
def sumup(police_number: int, something: int):
    assert police_number == 155
    assert something == 145

    return 155 + 145

@dataFrameLoader
def load_data3():
    return 111

@dataFrameSaver(sumup)
def save(result: int):
    assert result == 300
