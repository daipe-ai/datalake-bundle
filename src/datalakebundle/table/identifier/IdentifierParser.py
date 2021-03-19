class IdentifierParser:
    def parse(self, identifier: str) -> dict:
        dot_position = identifier.find(".")

        if dot_position == -1:
            raise Exception("Identifier must meet the following format: {db_identifier}.{table_identifier}")

        return {
            "db_identifier": identifier[:dot_position],
            "table_identifier": identifier[dot_position + 1 :],  # noqa: E203
            "identifier": identifier,
        }
