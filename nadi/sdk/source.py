from nadi.sdk.input import RuntimeArguments
from nadi.sdk.stream import RestStream, Stream
from nadi.sdk.config import Configs


class CatalogInputIsRequiredError(Exception):
    def __init__(self) -> None:
        message = "Catalog Input is required for this operation."
        super().__init__(message)


class StateInputIsRequiredError(Exception):
    def __init__(self) -> None:
        message = "State Input is required for this operation."
        super().__init__(message)


class StreamNotSupportedError(Exception):
    def __init__(self, stream_name: str) -> None:
        message = f"Stream '{stream_name}' is not supported."
        super().__init__(message)


class Source:
    def __init__(self, name: str) -> None:
        self.name = name
        self.streams: list[Stream] = []

    def get_stream(self, stream_name: str) -> Stream:
        for stream in self.streams:
            if stream.name == stream_name:
                return stream
        raise StreamNotSupportedError(stream_name)

    def _write(self, output: dict[str, object] | list[dict[str, object]]):
        if Configs.get_or_error("nadi.output.to") == "stdout":
            if isinstance(output, dict):
                print(output)
            else:
                for line in output:
                    print(line)

    def fetch_all(self, dry_run: bool = False):
        if RuntimeArguments.catalog is None:
            raise CatalogInputIsRequiredError()

        for catalog in RuntimeArguments.catalog.json_lines_data:
            catalog_configs = catalog.configs if catalog.configs is not None else {}
            self.fetch_stream(
                catalog.name, override_configs=catalog_configs, dry_run=dry_run
            )

    def fetch_stream(
        self,
        stream_name: str,
        override_configs: "dict[str, object] | None" = None,
        dry_run: bool = False,
    ):
        stream = self.get_stream(stream_name)
        if dry_run:
            if isinstance(stream, RestStream):
                stream.prepare_requests(override_configs=override_configs)
            return

        for data in stream.fetch(override_configs=override_configs):
            self._write(data)
