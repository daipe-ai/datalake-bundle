# pylint: disable = invalid-name
from injecta.parameter.ParametersParser import ParametersParser
import yaml

class AclConfigReader:

    def read_config(self, yaml_path: str):
        parsedYamlConfig = yaml.safe_load(self.__read_yaml(yaml_path))
        config = ParametersParser().parse(parsedYamlConfig)
        return config

    def __read_yaml(self, file_path: str):
        yml_file = open(file_path, "r")
        yamlText = yml_file.read()
        yml_file.close()
        return yamlText
