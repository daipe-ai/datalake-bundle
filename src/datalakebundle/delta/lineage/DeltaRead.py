from datalakebundle.lineage.FileRead import FileRead


class DeltaRead(FileRead):
    def __init__(self, path: str):
        super().__init__(path, "delta")
