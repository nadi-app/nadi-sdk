from nadi.sdk.auth import Auth, RestAuth
from nadi.sdk.input import RuntimeArguments
from nadi.sdk.stream import RestStream, Stream
from nadi.sdk.config import Conf, Configs


class CatalogInputIsRequiredError(Exception):
    def __init__(self) -> None:
        message = "Catalog Input is required for this operation."
        super().__init__(message)


class StateInputIsRequiredError(Exception):
    def __init__(self) -> None:
        message = "State Input is required for this operation."
        super().__init__(message)


class StreamNotSupportedError(Exception):
    def __init__(self, stream_name: str, supported_Streams: list[str]) -> None:
        message = f"Stream '{stream_name}' is not supported. Supported streams are {supported_Streams}"
        super().__init__(message)


class AuthCannotBePerformed(Exception):
    def __init__(self, auth_methods: list[str]) -> None:
        message = f"Auth cannot be performed. All available auth methods {auth_methods} were unsuccessful."
        super().__init__(message)


class EnforcedAuthIsNotSupported(Exception):
    def __init__(self, selected_auth: str) -> None:
        message = f"Selected auth '{selected_auth}' is not available for source."
        super().__init__(message)


class Source:
    def __init__(self, name: str) -> None:
        self.name = name
        self.supported_streams: list[Stream] = []
        self.supported_auths: list[Auth] = []

    @property
    def supported_configs(self) -> list[Conf]:
        return self.__supported_configs

    @supported_configs.setter
    def supported_configs(self, configs: list[Conf]):
        for conf in configs:
            Configs.add_supported_config(conf)
        self.__supported_configs = configs

    def get_stream(self, stream_name: str) -> Stream:
        for stream in self.supported_streams:
            if stream.name == stream_name:
                return stream
        raise StreamNotSupportedError(
            stream_name, [stream.name for stream in self.supported_streams]
        )

    def get_auth(self) -> Auth:
        if (
            enforce_method := Configs.get_or_error("nadi.auth.enforce_method")
        ) != "NONE":
            for auth in self.supported_auths:
                if enforce_method == auth.name:
                    return auth
            raise EnforcedAuthIsNotSupported(str(enforce_method))

        for auth in self.supported_auths:
            if isinstance(auth, RestAuth) and auth.can_prepare_request():
                return auth
        raise AuthCannotBePerformed([auth.name for auth in self.supported_auths])

    def _write(self, output: dict[str, object] | list[dict[str, object]]):
        if Configs.get("nadi.output.to") == "stdout":
            if isinstance(output, dict):
                print(output)
            else:
                for line in output:
                    print(line)

    def fetch_all(self, limit: int | None = None, dry_run: bool = False):
        if RuntimeArguments.catalog is None:
            raise CatalogInputIsRequiredError()

        for catalog in RuntimeArguments.catalog.json_lines_data:
            RuntimeArguments.catalog.set_stream_config(
                catalog.configs if catalog.configs is not None else {}
            )
            self.fetch_stream(catalog.name, limit=limit, dry_run=dry_run)
            RuntimeArguments.catalog.reset_stream_config()

    def fetch_stream(
        self,
        stream_name: str,
        limit: int | None = None,
        dry_run: bool = False,
    ):
        stream = self.get_stream(stream_name)
        auth = self.get_auth()

        if dry_run:
            if isinstance(stream, RestStream) and isinstance(auth, RestAuth):
                stream.prepare_requests(auth=auth)
            return

        for data in stream.fetch(auth, limit):
            self._write(data)
