from datalakebundle.lineage.FileRead import FileRead


class JsonRead(FileRead):
    def __init__(self, path: str):
        super().__init__(path, "json")
