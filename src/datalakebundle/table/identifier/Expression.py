# pylint: disable = invalid-name
import yaml
from simpleeval import simple_eval

class Expression:
    yaml_tag = '!expr'
    yaml_loader = yaml.SafeLoader

    def __init__(self, val):
        self.val = val

    def evaluate(self, variables: dict):
        return simple_eval(self.val, names=variables)

    @classmethod
    def from_yaml(cls, loader, node): # pylint: disable = unused-argument
        return cls(node.value)
