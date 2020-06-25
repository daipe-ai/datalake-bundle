import re
from datalakebundle.table.identifier.IdentifierParserInterface import IdentifierParserInterface

class IdentifierParser(IdentifierParserInterface):

    def __init__(self, identifierMatcher: str):
        self.__identifierMatcher = identifierMatcher

    def parse(self, identifier: str):
        escapedTemplate = re.escape(self.__identifierMatcher)
        escapedTemplate = re.sub(r'\\{([A-Za-z_]+)\\}', '(?P<\\1>[a-zA-Z0-9_]+)', escapedTemplate)

        matches = re.match(re.compile(escapedTemplate), identifier)

        return matches.groupdict()
