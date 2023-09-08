import contextlib
from abc import abstractmethod
from requests import Request
from nadi.sdk.config import (
    ConfigIsAlreadySupported,
    ConfigNotFoundError,
    Configs,
    StringConf,
)


class Auth:
    def __init__(self, name: str) -> None:
        self.name = name


class RestAuth(Auth):
    def __init__(self, name: str) -> None:
        with contextlib.suppress(ConfigIsAlreadySupported):
            Configs.add_supported_config(
                StringConf(
                    "nadi.auth.enforce_method",
                    "NONE",
                    is_required=False,
                    is_secret=False,
                    valid_values=["NONE", "BASIC", "BEARER", "NO_AUTH"],
                )
            )
        super().__init__(name)

    @abstractmethod
    def prepare_request(self, request: Request) -> Request:
        pass

    def can_prepare_request(self) -> bool:
        try:
            self.prepare_request(Request())
        except ConfigNotFoundError:
            return False
        return True


class NoRestAuth(RestAuth):
    def __init__(self) -> None:
        super().__init__("NO_AUTH")

    def prepare_request(self, request: Request) -> Request:
        return request


class BasicAuth(RestAuth):
    def __init__(self) -> None:
        Configs.add_supported_config(
            StringConf("nadi.auth.basic.username", None, is_required=False)
        )
        Configs.add_supported_config(
            StringConf("nadi.auth.basic.password", None, is_required=False),
        )
        super().__init__("BASIC")

    def prepare_request(self, request: Request) -> Request:
        request.auth = (
            Configs.get_or_error("nadi.auth.basic.username"),
            Configs.get_or_error("nadi.auth.basic.password"),
        )
        return request


class BearerAuth(RestAuth):
    def __init__(self) -> None:
        Configs.add_supported_config(
            StringConf("nadi.auth.bearer.token", None, is_required=False)
        )

        super().__init__("BEARER")

    def prepare_request(self, request: Request) -> Request:
        request.headers[
            "Authorization"
        ] = f"BEARER {Configs.get_or_error('nadi.auth.bearer.token')}"
        return request
