class IdentifierParser:

    def parse(self, identifier: str) -> dict:
        dotPosition = identifier.find('.')

        if dotPosition == -1:
            raise Exception('Identifier must meet the following format: {dbIdentifier}.{tableIdentifier}')

        return {
            'dbIdentifier': identifier[:dotPosition],
            'tableIdentifier': identifier[dotPosition + 1:],
            'identifier': identifier,
        }
