from abc import abstractmethod
from typing import Generator
from nadi.sdk.config import Configs
from nadi.sdk.util import Util
from copy import deepcopy
from requests import Response, Request, Session
from requests.exceptions import ChunkedEncodingError


class StreamDoesNotHaveOutputSchemaError(Exception):
    def __init__(self, stream_name: str) -> None:
        message = (
            f"Stream '{stream_name}' does not have json_schema for output validation."
        )
        super().__init__(message)


class StreamMissingArgumentConfigMapError(Exception):
    def __init__(self, stream_name: str, argument: str) -> None:
        message = f"Stream '{stream_name}' does not have 'argument_config_map' entry for {argument}."
        super().__init__(message)


class StreamResponseStatusInvalid(Exception):
    def __init__(self, stream_name: str, response: Response) -> None:
        message = f"Stream '{stream_name}' has invalid response. [Status '{response.status_code} - {response.reason}' for url '{response.url}']"
        super().__init__(message)


class StreamResponseContentInvalid(Exception):
    def __init__(self, stream_name: str) -> None:
        message = f"Stream '{stream_name}' has invalid content in response."
        super().__init__(message)


class ConfigMap:
    def __init__(
        self, short_key: str, config_key: str, is_required: bool = True
    ) -> None:
        self.short_key = short_key
        self.config_key = config_key
        self.is_required = is_required


class Stream:
    def __init__(
        self,
        name: str,
        description: str,
        output_json_schema: "str | dict[str, object] | None" = None,
    ) -> None:
        self.name = name
        self.description = description
        self.output_json_schema = (
            Util.read_json_file(output_json_schema)
            if isinstance(output_json_schema, str)
            else output_json_schema
        )

    def _replace_arguments_with_value(
        self, fmt_string: str, override_configs: dict[str, object] | None = None
    ) -> str:
        format_args = Util.extract_format_args_from_string(fmt_string)
        format_dict = {
            key: Configs.get_or_error(key, override_configs) for key in format_args
        }
        return fmt_string.format(**format_dict)

    def validate_schema(self, json_data: "dict[str, object] | list[dict[str, object]]"):
        if self.output_json_schema is not None:
            Util.validate_against_schema(json_data, self.output_json_schema)
        if Configs.get_or_error("nadi.output.enable_schema_validation"):
            raise StreamDoesNotHaveOutputSchemaError(self.name)

    def discover(self) -> dict[str, object]:
        return {"name": self.name}

    @abstractmethod
    def required_configs(self) -> set[str]:
        raise NotImplementedError(
            "'required_configs' method has to be implemented by child class."
        )

    @abstractmethod
    def fetch(
        self, override_configs: dict[str, object] | None = None
    ) -> Generator[dict[str, object] | list[dict[str, object]], None, None]:
        raise NotImplementedError(
            "'fetch' method has to be implemented by child class."
        )


class RestStream(Stream):
    def __init__(
        self,
        name: str,
        description: str,
        request: Request,
        output_json_schema: "str | dict[str, object] | None" = None,
    ) -> None:
        super().__init__(name, description, output_json_schema)
        self.original_request = request

    def prepare_requests(self, override_configs: "dict[str, object] | None") -> Request:
        request = deepcopy(self.original_request)
        request.url = self._replace_arguments_with_value(request.url, override_configs)
        for key in request.params:
            request.params[key] = self._replace_arguments_with_value(
                request.params[key], override_configs
            )

        for key in request.headers:
            request.headers[key] = self._replace_arguments_with_value(
                request.headers[key], override_configs
            )

        return request

    def fetch(
        self, override_configs: dict[str, object] | None = None
    ) -> Generator[dict[str, object] | list[dict[str, object]], None, None]:
        request = self.prepare_requests(override_configs=override_configs)
        response = None

        session = Session()
        while (request := self.fetch_next_request(request, response)) is not None:
            prepared_request = request.prepare()
            try:
                with session.send(prepared_request, stream=False) as response:
                    if response.status_code != 200:
                        raise StreamResponseStatusInvalid(self.name, response)
                    json_response = response.json()
                    self.validate_schema(json_response)
                yield json_response
            except ChunkedEncodingError as err:
                raise StreamResponseContentInvalid(self.name) from err

    @abstractmethod
    def fetch_next_request(
        self,
        previous_request: Request,
        previous_response: Response | None,
    ) -> "Request| None":
        raise NotImplementedError(
            "'fetch_next_request' method has to be implemented by child class."
        )