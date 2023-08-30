from enum import Enum
from typing import Any


class InvalidConfigType(Exception):
    def __init__(self, key: str, actual_type: type, expected_type: type) -> None:
        message = f"Provided Key={key} is of invalid type. Expected Type : {expected_type} - Actual Type : {actual_type}"
        super().__init__(message)


class ConfigNotDefinedError(Exception):
    def __init__(self, key: str) -> None:
        message = f"Unable to find any config with Key={key}"
        super().__init__(message)


class OUTPUT_FORMAT(Enum):
    JSON_LINES = 0
    JSON = 1


class OUTPUT_TYPE(Enum):
    STDOUT = 0
    RETURN = 1


class C:
    def __init__(self, key: str, value: Any, value_type: type) -> None:
        self.key = key
        self.value_type = value_type
        self.__validate_type(value)
        self.__value = value

    @property
    def value(self):
        return self.__value

    def __validate_type(self, val: Any):
        if not isinstance(val, self.value_type):
            raise InvalidConfigType(self.key, type(val), self.value_type)

    def put(self, value: Any):
        self.__validate_type(value)
        self.__value = value


class CONFIGS:
    nadi_prefix_arg = C("nadi_sdk.prefix.arg", "arg", str)
    nadi_prefix_param = C("nadi_sdk.prefix.param", "param", str)
    nadi_prefix_header = C("nadi_sdk.prefix.header", "header", str)
    nadi_stream_parallelism = C("nadi_sdk.stream.parallelism", 5, int)
    nadi_stream_file_write_frequency = C(
        "nadi_sdk.stream.file.write_frequency", 10, int
    )
    nadi_stream_request_limit = C("nadi_sdk.stream.request_limit", 999, int)
    nadi_output_type = C("nadi_sdk.output_type", OUTPUT_TYPE.STDOUT, OUTPUT_TYPE)
    nadi_output_format = C(
        "nadi_sdk.output_format", OUTPUT_FORMAT.JSON_LINES, OUTPUT_FORMAT
    )

    __config: dict[str, C] = {
        nadi_prefix_arg.key: nadi_prefix_arg,
        nadi_prefix_param.key: nadi_prefix_param,
        nadi_prefix_header.key: nadi_prefix_header,
        nadi_stream_parallelism.key: nadi_stream_parallelism,
        nadi_stream_file_write_frequency.key: nadi_stream_file_write_frequency,
        nadi_stream_request_limit.key: nadi_stream_request_limit,
        nadi_output_type.key: nadi_output_type,
        nadi_output_format.key: nadi_output_format,
    }

    @classmethod
    def put(cls, key: str, value: Any, value_type: type = str):
        if (c := cls.__config.get(key)) is not None:
            c.put(value)
        else:
            cls.__config[key] = C(key, value, value_type)

    @classmethod
    def get(cls, key: str) -> Any:
        if (value := cls.__config.get(key)) is not None:
            return value.value
        raise ConfigNotDefinedError(key)
