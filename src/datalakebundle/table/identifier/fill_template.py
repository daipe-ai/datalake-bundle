import re


def fill_template(template: str, replacements: dict):
    def convert(matches):
        placeholder = matches.group(1)

        if placeholder not in replacements:
            raise Exception("Value for placeholder {" + placeholder + "} not defined")

        return str(replacements[placeholder])

    return re.sub(r"\{([^\}]+)\}", convert, template)
