import yaml
from simpleeval import simple_eval


class Expression:
    yaml_tag = "!expr"
    yaml_loader = yaml.SafeLoader

    def __init__(self, val):
        self.val = val

    @classmethod
    def from_yaml(cls, loader, node):
        return cls(node.value)

    def evaluate(self, variables: dict):
        return simple_eval(self.val, names=variables)
