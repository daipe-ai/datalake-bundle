from daipecore.lineage.argument.ArgumentMappingInterface import ArgumentMappingInterface
from datalakebundle.notebook.lineage.argument.ReadTable import ReadTable
from datalakebundle.notebook.lineage.argument.TableParams import TableParams


class ArgumentMapping(ArgumentMappingInterface):
    def get_mapping(self):
        return {
            "table_params": TableParams,
            "read_table": ReadTable,
        }
