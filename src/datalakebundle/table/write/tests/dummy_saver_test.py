import os
from daipecore.decorator.notebook_function import notebook_function
from datalakebundle.table.write.tests.dummy_saver import dummy_saver, TestingStorage

os.environ["APP_ENV"] = "test"


@notebook_function()
@dummy_saver("my_sample_table")
def load_data():
    return 155


@notebook_function(load_data)
def process(number: int):
    assert number == 155
    assert TestingStorage.result == 155
