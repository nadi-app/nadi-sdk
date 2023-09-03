from json import load, loads
from string import Formatter
from typing import Any
from jsonschema import validate
from jsonpath_ng import parse  # type: ignore


class Util:
    @staticmethod
    def read_json_file(json_path: str) -> "dict[str, object]":
        with open(json_path, "r") as json_file:
            return load(json_file)

    @staticmethod
    def read_json_lines_file(json_path: str) -> "list[dict[str, object]]":
        with open(json_path, "r") as json_lines_file:
            return [loads(json_line) for json_line in json_lines_file]

    @staticmethod
    def validate_against_schema(instance: Any, schema: dict[str, object]):
        validate(instance, schema)

    @staticmethod
    def filter_records_by_json_path(records: Any, json_path: str) -> list[Any]:
        jsonpath_expr = parse(json_path)  # type: ignore
        return [
            match.value  # type: ignore
            for match in jsonpath_expr.find(records)  # type: ignore
        ]

    @staticmethod
    def extract_format_args_from_string(string: str) -> set[str]:
        return {
            field for _, field, _, _ in Formatter().parse(string) if field is not None
        }
