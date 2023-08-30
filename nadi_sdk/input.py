from typing import Any
from nadi_sdk.config import (
    CONFIGS,
    OUTPUT_FORMAT,
    OUTPUT_TYPE,
)
from nadi_sdk.util import Util
from jsonschema import ValidationError

import re
from os import environ


class InputSchemaInvalidError(Exception):
    def __init__(
        self,
        *,
        input_type: str = "input",
        input_path: "str | None" = None,
    ) -> None:
        from_where = (
            input_type if input_path is None else f"{input_type} : {input_path}"
        )
        message = f"Schema is Invalid for {from_where}."
        super().__init__(message)


class InputSchemaMissingError(Exception):
    def __init__(
        self,
        *,
        input_type: str = "input",
        input_path: "str | None" = None,
    ) -> None:
        from_where = (
            input_type if input_path is None else f"{input_type} : {input_path}"
        )
        message = (
            f"Unable to validate schema, json schema is not provided for {from_where}."
        )
        super().__init__(message)


class InputMissingRequiredConfigError(Exception):
    def __init__(self, key: str) -> None:
        message = f"Required config '{key}' is missing."
        super().__init__(message)


class SourceInput:
    def __init__(self, input: "str | dict[str, object]") -> None:
        self.__input = self.__process_input(input)
        self.input_type = "input"
        self.input_path = input if isinstance(input, str) else None
        self.input_schema: "str | dict[str, object] | None" = None

    def validate_schema(self):
        try:
            if self.input_schema is None:
                raise InputSchemaMissingError(
                    input_type=self.input_type, input_path=self.input_path
                )
            if isinstance(self.input_schema, str):
                self.input_schema = Util.read_json_file(self.input_schema)
            Util.validate_json_against_schema(self.__input, self.input_schema)
        except ValidationError as verr:
            raise InputSchemaInvalidError(
                input_type=self.input_type, input_path=self.input_path
            ) from verr

    def __process_input(self, input: str | dict[str, object]) -> dict[str, object]:
        return Util.read_json_file(input) if isinstance(input, str) else input

    @property
    def input(self) -> dict[str, Any]:
        return {k: self.get(k) for k in self.__input}

    @input.setter
    def input(self, val: str | dict[str, object]):
        self.__input = self.__process_input(val)

    def get(self, key: str) -> "Any | None":
        return self.__input.get(key, environ.get(key))

    def get_or_error(self, key: str) -> "Any":
        _val = self.get(key)
        if _val is None:
            raise InputMissingRequiredConfigError(key)
        return _val

    def put(self, key: str, val: Any):
        self.__input[key] = val


class Config(SourceInput):
    def __init__(self, config: "str | dict[str, object]") -> None:
        super().__init__(config)
        self.input_type = "config"

    def get(self, key: str) -> "Any | None":
        return CONFIGS.get(key) if (_val := super().get(key)) is None else _val

    @property
    def arguments(self) -> dict[str, Any]:
        # sourcery skip: use-fstring-for-concatenation
        pattern = re.compile(
            (r"^.*?[.^]" + re.escape(CONFIGS.nadi_prefix_arg.value) + r"\.(.*)")
        )  # ^.*[^\.]arg\.(.*)
        output: dict[str, Any] = {}
        for key in self.input:
            arg = pattern.search(key)
            if arg is not None:
                output[arg[1]] = self.get(key)
        return output

    @property
    def output_type(self) -> OUTPUT_TYPE:
        _val = self.get_or_error(CONFIGS.nadi_output_type.key)
        return OUTPUT_TYPE[_val] if isinstance(_val, str) else _val

    @property
    def output_format(self) -> OUTPUT_FORMAT:
        _val = self.get_or_error(CONFIGS.nadi_output_format.key)
        return OUTPUT_FORMAT[_val] if isinstance(_val, str) else _val


class Catalog(SourceInput):
    def __init__(self, catalog: "str | dict[str, object]") -> None:
        super().__init__(catalog)
        self.input_type = "catalog"


class State(SourceInput):
    def __init__(self, state: "str | dict[str, object]") -> None:
        super().__init__(state)
        self.input_type = "state"
