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

@dataFrameSaver(sumup)
def save(result: int):
    assert result == 300

if __name__ == '__main__':
    assert isinstance(load_data, dataFrameLoader)
    assert load_data.result == 155
    assert globals()['load_data_df'] == 155

    assert isinstance(load_data2, dataFrameLoader)
    assert load_data2.result == 145
    assert globals()['load_data2_df'] == 145

    assert isinstance(sumup, transformation)
    assert sumup.result == 300
    assert globals()['sumup_df'] == 300

    assert isinstance(save, dataFrameSaver)
    assert save.result is None
    assert 'save_df' not in globals()
