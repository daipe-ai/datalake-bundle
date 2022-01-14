from datalakebundle.lineage.FileRead import FileRead


class ParquetRead(FileRead):
    def __init__(self, path: str):
        super().__init__(path, "parquet")
