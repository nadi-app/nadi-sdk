from abc import abstractmethod
from json import JSONDecodeError
from string import Formatter
from typing import Any
from jsonschema import ValidationError, validate

from requests import Response, get

from nadi.util import Util


class StreamMissingRequiredArguments(Exception):
    def __init__(self, argument: str) -> None:
        message = f"Required argument '{argument}' is missing."
        super().__init__(message)


class RestStreamResponseInvalidError(Exception):
    def __init__(self, url: str, error_code: int, error_body: str) -> None:
        message = (
            f"Rest call to '{url}' returned CODE: {error_code}, BODY: {error_body}"
        )
        super().__init__(message)


class StreamNotSupportedError(Exception):
    def __init__(self, stream_name: str) -> None:
        message = f"Stream '{stream_name}' is not supported."
        super().__init__(message)


class StreamOutputSchemaMissingError(Exception):
    def __init__(self, stream_name: str) -> None:
        message = f"Stream '{stream_name}' does not have any schema for validation."

        super().__init__(message)


class StreamOutputSchemaInvalidError(Exception):
    def __init__(self, stream_name: str) -> None:
        message = f"Schema validation failed for stream '{stream_name}'."

        super().__init__(message)


class Stream:
    def __init__(
        self,
        name: str,
        description: str,
        *,
        arguments: "dict[str, str] | None" = None,
        output_schema: "str | dict[str, object] | None" = None,
        filter_json_path: "str | None" = None,
    ) -> None:
        self.name = name
        self.description = description
        self.arguments = arguments if arguments is not None else {}
        self.output_schema = output_schema
        self.filter_json_path = filter_json_path

    @property
    def required_arguments(self) -> dict[str, object]:
        return {}

    @abstractmethod
    def fetch_records(self) -> Any:
        pass

    def filter_records(self, json_records: Any) -> Any:
        if self.filter_json_path is not None:
            return Util.filter_records_by_json_path(json_records, self.filter_json_path)
        return json_records

    def validate_required_arguments(self):
        for key in self.required_arguments:
            if key not in self.arguments:
                raise StreamMissingRequiredArguments(key)

    def validate_output_schema(self, output: Any):
        if self.output_schema is None:
            raise StreamOutputSchemaMissingError(self.name)
        if isinstance(self.output_schema, str):
            self.output_schema = Util.read_json_file(self.output_schema)
        try:
            validate(output, self.output_schema)
        except ValidationError as verr:
            raise StreamOutputSchemaInvalidError(self.name) from verr

    def inject_arguments(self, arguments: dict[str, str]):
        self.arguments = dict(self.arguments, **arguments)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "passed_arguments": self.arguments,
        }

    def to_catalog(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "arguments": self.required_arguments,
        }


class StreamCollection:
    def __init__(self) -> None:
        self.__streams: list[Stream] = []

    def add_stream(self, stream: Stream):
        self.__streams.append(stream)

    def inject_arguments(self, arguments: dict[str, str]):
        for stream in self.__streams:
            stream.inject_arguments(arguments)

    def inject_params(self, params: dict[str, str]):
        for stream in self.__streams:
            if isinstance(stream, RestStream):
                stream.inject_params(params)

    def inject_headers(self, headers: dict[str, str]):
        for stream in self.__streams:
            if isinstance(stream, RestStream):
                stream.inject_headers(headers)

    def get_stream_by_name(self, stream_name: str) -> "Stream | None":
        for stream in self.__streams:
            if stream.name == stream_name:
                return stream

    def get_stream_by_name_or_error(self, stream_name: str) -> Stream:
        if (stream := self.get_stream_by_name(stream_name)) is None:
            raise StreamNotSupportedError(stream_name)
        return stream

    def get_all(self) -> list[Stream]:
        return self.__streams


class RestStream(Stream):
    def __init__(
        self,
        name: str,
        description: str,
        url: str,
        *,
        arguments: "dict[str, str] | None" = None,
        output_schema: "str|dict[str, object] | None" = None,
        filter_json_path: "str|None" = None,
        headers: "dict[str, str]|None" = None,
        params: "dict[str, str]|None" = None,
    ) -> None:
        super().__init__(
            name,
            description,
            arguments=arguments,
            output_schema=output_schema,
            filter_json_path=filter_json_path,
        )
        self.url = url
        self.headers = headers if headers is not None else {}
        self.params = params if params is not None else {}

        self.previous_response: "Response|None" = None
        self.next_url: "str|None" = None

    def __extract_fmt_args_from_url(self) -> set[str]:
        return {
            field
            for _, field, _, _ in Formatter().parse(self.current_url)
            if field is not None
        }

    def inject_params(self, params: dict[str, str]):
        self.params = dict(self.params, **params)

    def inject_headers(self, headers: dict[str, str]):
        self.headers = dict(self.headers, **headers)

    def reset(self):
        self.previous_response = None
        self.next_url = None

    def fetch_records(self) -> Any:
        self.validate_required_arguments()
        formatted_url = self.current_url.format(**self.arguments)

        resp = get(formatted_url, self.params, headers=self.headers)
        self.previous_response = resp

        if resp.status_code != 200:
            raise RestStreamResponseInvalidError(
                formatted_url, resp.status_code, str(resp.content)
            )
        try:
            resp_json = resp.json()
        except JSONDecodeError as jderr:
            raise RestStreamResponseInvalidError(
                formatted_url, resp.status_code, str(resp.content)
            ) from jderr

        resp_json = self.filter_records(resp_json)
        self.validate_output_schema(resp_json)
        return resp_json

    def prepare_next_url(self) -> bool:
        return False

    @property
    def current_url(self) -> str:
        return self.url if self.next_url is None else self.next_url

    @property
    def required_arguments(self) -> dict[str, object]:
        return {
            key: "" if key not in self.arguments else self.arguments[key]
            for key in self.__extract_fmt_args_from_url()
        }

    @property
    def required_headers(self) -> dict[str, object]:
        return {}

    @property
    def required_params(self) -> dict[str, object]:
        return {}

    def to_dict(self) -> dict[str, object]:
        return dict(
            super().to_dict(),
            **{
                "url": self.current_url,
                "passed_params": self.params,
                "passed_headers": self.headers,
            },
        )

    def to_catalog(self) -> dict[str, Any]:
        all_options = dict(
            super().to_catalog(),
            **{
                "params": self.required_params,
                "headers": self.required_headers,
            },
        )
        return {k: v for k, v in all_options.items() if len(v) > 0}
