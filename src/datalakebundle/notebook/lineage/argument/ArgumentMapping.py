from daipecore.lineage.argument.ArgumentMappingInterface import ArgumentMappingInterface
from datalakebundle.notebook.lineage.argument.ReadTable import ReadTable
from datalakebundle.notebook.lineage.argument.TableParams import TableParams
from datalakebundle.csv.lineage.CsvRead import CsvRead
from datalakebundle.delta.lineage.DeltaRead import DeltaRead
from datalakebundle.json.lineage.JsonRead import JsonRead
from datalakebundle.parquet.lineage.ParquetRead import ParquetRead


class ArgumentMapping(ArgumentMappingInterface):
    def get_mapping(self):
        return {
            "table_params": TableParams,
            "read_table": ReadTable,
            "read_csv": CsvRead,
            "read_json": JsonRead,
            "read_parquet": ParquetRead,
            "read_delta": DeltaRead,
        }
