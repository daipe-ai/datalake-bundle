from datalakebundle.lineage.FileRead import FileRead


class CsvRead(FileRead):
    def __init__(self, path: str):
        super().__init__(path, "csv")
