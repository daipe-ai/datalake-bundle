from typing import List
from box import Box
from injecta.dtype.DType import DType
from injecta.service.argument.TaggedAliasedServiceArgument import TaggedAliasedServiceArgument
from injecta.service.Service import Service
from injecta.service.ServiceAlias import ServiceAlias
from injecta.service.argument.PrimitiveArgument import PrimitiveArgument
from injecta.service.argument.ServiceArgument import ServiceArgument
from pyfonybundles.Bundle import Bundle
from daipecore.detector import is_cli
from datalakebundle.table.identifier.Expression import Expression
from datalakebundle.lineage.PathWriterParser import PathWriterParser
from datalakebundle.read.PathReader import PathReader
from datalakebundle.write.PathWriter import PathWriter


Expression.yaml_loader.add_constructor(Expression.yaml_tag, Expression.from_yaml)


class DataLakeBundle(Bundle):
    def modify_raw_config(self, raw_config: dict) -> dict:
        table_defaults = raw_config["parameters"]["datalakebundle"]["table"]["defaults"]
        if table_defaults:
            self.__check_defaults(table_defaults)

        return raw_config

    def modify_services(self, services: List[Service], aliases: List[ServiceAlias], parameters: Box):
        formats = ["delta", "parquet", "json", "csv"]

        path_readers = [self.__create_path_reader(format_name) for format_name in formats]
        path_writers = [self.__create_path_writer(format_name) for format_name in formats]
        decorator_parsers = [
            self.__create_decorator_parser(format_name, operation) for format_name in formats for operation in ["append", "overwrite"]
        ]

        # backward compatibility, will be removed in 2.0
        for service in services:
            if service.name == "datalakebundle.filesystem.FilesystemInjector":
                service.arguments[0] = TaggedAliasedServiceArgument("pysparkbundle.filesystem", parameters.pysparkbundle.filesystem)
            if service.name == "datalakebundle.dataframe.DataFrameShowMethodInjector":
                service.arguments[0] = TaggedAliasedServiceArgument(
                    "pysparkbundle.dataframe.show_method", parameters.pysparkbundle.dataframe.show_method
                )

        return services + path_readers + path_writers + decorator_parsers, aliases

    def modify_parameters(self, parameters: Box) -> Box:
        if is_cli():
            parameters.datalakebundle.dataframe.show_method = "dataframe_show"

            # backward compatibility, will be removed in 2.0
            if "pysparkbundle" in parameters:
                parameters.pysparkbundle.dataframe.show_method = "dataframe_show"

        return parameters

    def __check_defaults(self, table_defaults: dict):
        for field in ["identifier", "db_identifier", "db_name", "table_identifier", "table_name"]:
            if field in table_defaults:
                raise Exception(f"datalakebundle.table.defaults.{field} parameter must not be explicitly defined")

    def __create_path_reader(self, format_name: str):
        return Service(
            f"datalakebundle.{format_name}.reader",
            DType(PathReader.__module__, "PathReader"),
            [PrimitiveArgument(format_name), ServiceArgument("datalakebundle.logger")],
        )

    def __create_path_writer(self, format_name: str):
        return Service(
            f"datalakebundle.{format_name}.writer",
            DType(PathWriter.__module__, "PathWriter"),
            [PrimitiveArgument(format_name), ServiceArgument("datalakebundle.logger")],
        )

    def __create_decorator_parser(self, format_name: str, operation: str):
        return Service(
            f"datalakebundle.lineage.{format_name}.parser.{operation}",
            DType(PathWriterParser.__module__, "PathWriterParser"),
            [PrimitiveArgument(format_name + "_" + operation), PrimitiveArgument(operation)],
            tags=["lineage.decorator.parser"],
        )
