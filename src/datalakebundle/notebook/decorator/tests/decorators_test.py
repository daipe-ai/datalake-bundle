import io
import os
from contextlib import redirect_stdout
import pandas as pd
from datalakebundle.notebook.decorator.data_frame_loader import data_frame_loader
from datalakebundle.notebook.decorator.transformation import transformation
from datalakebundle.notebook.decorator.data_frame_saver import data_frame_saver

os.environ["APP_ENV"] = "test"

printed = """   a  b
0  0  2
1  1  3
"""


@data_frame_loader()
def load_data():
    return pd.DataFrame({"a": [0, 1]})


@transformation()
def load_data2():
    return pd.DataFrame({"b": [2, 3]})


@transformation(load_data, load_data2, check_duplicate_columns=False)
def concat(df1: pd.DataFrame, df2: pd.DataFrame):
    assert df1["a"].values.tolist() == [0, 1]
    assert df2["b"].values.tolist() == [2, 3]

    return pd.concat([df1, df2], axis=1)


@data_frame_saver(concat)
def save(result: pd.DataFrame):
    assert result.shape == (2, 2)


def get_list(df: pd.DataFrame, column: str):
    return df[column].values.tolist()


if __name__ == "__main__":
    assert isinstance(load_data, data_frame_loader)
    assert get_list(load_data.result, "a") == [0, 1]
    assert get_list(globals()["load_data_df"], "a") == [0, 1]

    assert isinstance(load_data2, transformation)
    assert get_list(load_data2.result, "b") == [2, 3]
    assert get_list(globals()["load_data2_df"], "b") == [2, 3]

    assert isinstance(concat, transformation)
    assert get_list(concat.result, "a") == [0, 1]
    assert get_list(concat.result, "b") == [2, 3]
    assert get_list(globals()["concat_df"], "a") == [0, 1]
    assert get_list(globals()["concat_df"], "b") == [2, 3]

    assert isinstance(save, data_frame_saver)
    assert save.result is None
    assert "save_df" not in globals()

    with redirect_stdout(io.StringIO()) as f:

        @transformation(load_data, load_data2, check_duplicate_columns=False, display=True)
        def concat(df1: pd.DataFrame, df2: pd.DataFrame):
            assert df1["a"].values.tolist() == [0, 1]
            assert df2["b"].values.tolist() == [2, 3]

            return pd.concat([df1, df2], axis=1)

    assert printed == f.getvalue()
