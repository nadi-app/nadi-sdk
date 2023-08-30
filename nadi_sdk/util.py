from json import load
from typing import Any
from jsonschema import validate
from jsonpath_ng import parse  # type: ignore


class Util:
    @staticmethod
    def read_json_file(json_path: str) -> dict[str, object]:
        with open(json_path, "r") as json_file:
            return load(json_file)

    @staticmethod
    def validate_json_against_schema(instance: Any, schema: dict[str, object]):
        validate(instance, schema)

    @staticmethod
    def filter_records_by_json_path(records: Any, json_path: str) -> list[Any]:
        jsonpath_expr = parse(json_path)  # type: ignore
        return [
            match.value  # type: ignore
            for match in jsonpath_expr.find(records)  # type: ignore
        ]
