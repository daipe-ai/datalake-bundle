import string


def extract(template: str):
    iterator = string.Formatter().parse(template)

    return {name for text, name, spec, conv in iterator if name is not None}
