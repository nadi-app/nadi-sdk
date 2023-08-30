from typing import Any, Generator
from json import dumps

from nadi_sdk.config import (
    CONFIGS,
    OUTPUT_FORMAT,
    OUTPUT_TYPE,
)
from nadi_sdk.input import (
    Config,
    Catalog,
    State,
)
from nadi_sdk.stream import RestStream, StreamCollection


class Options:
    def __init__(self) -> None:
        self.__config: Config = Config({})
        self.__catalog: Catalog = Catalog({})
        self.__state: State = State({})

    @property
    def config(self) -> Config:
        return self.__config

    @property
    def catalog(self) -> Catalog:
        return self.__catalog

    @property
    def state(self) -> State:
        return self.__state

    @config.setter
    def config(self, value: "str |dict[str, object]"):
        self.__config = Config(value)

    @catalog.setter
    def catalog(self, value: "str |dict[str, object]"):
        self.__catalog = Catalog(value)

    @state.setter
    def state(self, value: "str |dict[str, object]"):
        self.__state = State(value)


class Source:
    name = "ns"

    def __init__(self) -> None:
        self.options = Options()
        self.streams: StreamCollection = StreamCollection()

    def _write(
        self,
        output: list[dict[str, object]] | dict[str, object],
        output_type: OUTPUT_TYPE | None = None,
        output_format: OUTPUT_FORMAT | None = None,
    ) -> "dict[str, object] | list[dict[str, object]] | None":
        op_type = (
            output_type if output_type is not None else self.options.config.output_type
        )
        op_format = (
            output_format
            if output_format is not None
            else self.options.config.output_format
        )

        if op_type == OUTPUT_TYPE.RETURN and op_format in [
            OUTPUT_FORMAT.JSON,
            OUTPUT_FORMAT.JSON_LINES,
        ]:
            return output
        elif op_type == OUTPUT_TYPE.STDOUT:
            if op_format == OUTPUT_FORMAT.JSON:
                print(dumps(output))
            elif op_format == OUTPUT_FORMAT.JSON_LINES:
                for out in output:
                    print(dumps(out))

    def inject_arguments_from_config(self):
        self.streams.inject_arguments(self.options.config.arguments)

    def yield_stream_records(
        self, stream_name: str, *, request_limit: int | None = None
    ) -> "Generator[list[dict[str, object]], Any, Any]":
        stream = self.streams.get_stream_by_name_or_error(stream_name)
        config_stream_request_limit = (
            request_limit
            if request_limit is not None
            else self.options.config.get_or_error(CONFIGS.nadi_stream_request_limit.key)
        )

        if isinstance(stream, RestStream):
            request_count = 0
            while request_count < config_stream_request_limit:
                yield stream.fetch_records()
                request_count += 1
                if not stream.prepare_next_url():
                    break

    def fetch_stream(
        self, stream_name: str, *, request_limit: int | None = None
    ) -> "list[dict[str,object]] | None":
        output_records: list[dict[str, object]] = []
        for record in self.yield_stream_records(
            stream_name, request_limit=request_limit
        ):
            if (record := self._write(record)) is not None:
                if isinstance(record, dict):
                    output_records.append(record)
                else:
                    output_records.extend(record)

        if output_records:
            return output_records

    def describe_stream(
        self, stream_name: str
    ) -> "dict[str, object] | list[dict[str, object]] | None":
        stream = self.streams.get_stream_by_name_or_error(stream_name)
        return self._write(stream.to_dict())

    def discover_stream(
        self, stream_name: str
    ) -> "dict[str, object] | list[dict[str, object]] | None":
        stream = self.streams.get_stream_by_name_or_error(stream_name)
        return self._write(stream.to_catalog())

    def fetch(self, *, request_limit: int | None = None) -> dict[str, object]:
        raise NotImplementedError()

    def describe(self) -> "dict[str, object] | list[dict[str, object]] | None":
        output = [stream.to_dict() for stream in self.streams.get_all()]
        return self._write(output)

    def discover(self) -> "dict[str, object] | list[dict[str, object]] | None":
        output = [stream.to_catalog() for stream in self.streams.get_all()]
        return self._write(output, output_format=OUTPUT_FORMAT.JSON)
