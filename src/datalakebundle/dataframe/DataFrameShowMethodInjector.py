from datalakebundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface


class DataFrameShowMethodInjector:
    def __init__(self, data_frame_show: DataFrameShowMethodInterface):
        self.__data_frame_show = data_frame_show

    def get(self):
        return self.__data_frame_show
