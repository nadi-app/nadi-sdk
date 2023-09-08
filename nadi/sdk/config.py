import contextlib
from os import environ

from nadi.sdk.input import RuntimeArguments


class ConfigNotSupportedError(Exception):
    def __init__(self, key: str) -> None:
        message = f"Config '{key}' is not supported by application."
        super().__init__(message)


class ConfigNotFoundError(Exception):
    def __init__(self, key: str) -> None:
        message = f"Config '{key}' not provided."
        super().__init__(message)


class ConfigTypeInvalidError(Exception):
    def __init__(self, key: str, actual_type: type, expected_type: type) -> None:
        message = f"Provided config '{key}' is of invalid type. Expected Type is '{expected_type.__name__}' but actual type is '{actual_type.__name__}'."
        super().__init__(message)


class ConfigValueInvalidError(Exception):
    def __init__(self, key: str, extended_message: str) -> None:
        message = f"Provided value for config '{key}' is invalid. {extended_message}"
        super().__init__(message)


class ConfigIsAlreadySupported(Exception):
    def __init__(self, key: str, argument_key: str | None) -> None:
        super().__init__(
            f"Config with key '{key}' or argument_key {argument_key} is already present."
        )


class Conf:
    def __init__(
        self,
        key: str,
        default_value: "object | None",
        argument_key: "str | None" = None,
        is_secret: bool = True,
        is_required: bool = True,
    ) -> None:
        self.key = key
        self.default_value = default_value
        self.argument_key = argument_key
        self.is_secret = is_secret
        self.is_required = is_required

    def convert_to_type(self, value: "object|None", value_type: "type") -> object:
        if value is not None and not isinstance(value, value_type):
            try:
                value = value_type(value)
            except ValueError as e:
                raise ConfigTypeInvalidError(self.key, type(value), value_type) from e
        return value

    def validate(self, value: object) -> None:
        if self.is_required and value is None:
            raise ConfigValueInvalidError(
                self.key, "Given Value is Required, it cannot be None."
            )

    def to_dict(self) -> dict[str, object | None]:
        value = None
        with contextlib.suppress(ConfigValueInvalidError):
            value = Configs.get(self.key)
        value = "___REDACTED___" if self.is_secret and value is not None else value
        return {
            "key": self.key,
            "value": value,
            "argument_key": self.argument_key,
            "default_value": self.default_value,
            "is_secret": self.is_secret,
            "is_required": self.is_required,
        }


class StringConf(Conf):
    def __init__(
        self,
        key: str,
        default_value: str | None,
        argument_key: "str | None" = None,
        is_secret: bool = True,
        is_required: bool = True,
        valid_values: list[str] | None = None,
    ) -> None:
        self.valid_values = valid_values
        super().__init__(key, default_value, argument_key, is_secret, is_required)

    def validate(self, value: object) -> None:
        super().validate(value)
        self.convert_to_type(value, str)
        if self.valid_values is not None and value not in self.valid_values:
            raise ConfigValueInvalidError(
                self.key, f"Provided value {value} is not one of {self.valid_values}."
            )

    def to_dict(self) -> dict[str, object]:
        return dict(super().to_dict(), **{"valid_values": self.valid_values})


class IntConf(Conf):
    def __init__(
        self,
        key: str,
        default_value: int | None,
        argument_key: "str | None" = None,
        is_secret: bool = True,
        is_required: bool = True,
    ) -> None:
        super().__init__(key, default_value, argument_key, is_secret, is_required)

    def validate(self, value: object) -> None:
        super().validate(value)
        self.convert_to_type(value, int)


class FloatConf(Conf):
    def __init__(
        self,
        key: str,
        default_value: float | None,
        argument_key: "str | None" = None,
        is_secret: bool = True,
        is_required: bool = True,
    ) -> None:
        super().__init__(key, default_value, argument_key, is_secret, is_required)

    def validate(self, value: object) -> None:
        super().validate(value)
        self.convert_to_type(value, float)


class BooleanConf(Conf):
    def __init__(
        self,
        key: str,
        default_value: bool | None,
        argument_key: "str | None" = None,
        is_secret: bool = True,
        is_required: bool = True,
    ) -> None:
        super().__init__(key, default_value, argument_key, is_secret, is_required)

    def validate(self, value: object):
        super().validate(value)
        if isinstance(value, str) and value.lower() in ["true", "false"]:
            value = value.lower() == "true"
        if not isinstance(value, bool):
            raise ConfigTypeInvalidError(self.key, type(value), bool)


class Configs:
    supported_configs: list[Conf] = [
        StringConf(
            "nadi.output.format",
            "jsonlines",
            is_secret=False,
            valid_values=["json", "jsonlines"],
        ),
        StringConf(
            "nadi.output.to",
            "stdout",
            is_secret=False,
            valid_values=["stdout"],
        ),
        BooleanConf(
            "nadi.output.enable_schema_validation",
            True,
            is_secret=False,
        ),
    ]

    @classmethod
    def add_supported_config(cls, in_conf: Conf):
        for conf in cls.supported_configs:
            if conf.key == in_conf.key or (
                conf.argument_key is not None
                and in_conf.argument_key is not None
                and conf.argument_key == in_conf.argument_key
            ):
                raise ConfigIsAlreadySupported(in_conf.key, in_conf.argument_key)
        cls.supported_configs.append(in_conf)

    @classmethod
    def get_supported_conf(cls, key: str) -> Conf:
        for conf in Configs.supported_configs:
            if conf.key == key or (
                conf.argument_key is not None and conf.argument_key == key
            ):
                return conf
        raise ConfigNotSupportedError(key)

    @classmethod
    def get(cls, key: str) -> "object|None":
        def _get(conf: Conf, map: dict[str, object]):
            _value = (
                map.get(conf.argument_key) if conf.argument_key is not None else None
            )
            if _value is None:
                _value = map.get(conf.key)
            return _value

        conf = cls.get_supported_conf(key)
        value: "object | None" = None

        if RuntimeArguments.state is not None:
            value = _get(conf, RuntimeArguments.state.stream_config)
        if value is None and RuntimeArguments.catalog is not None:
            value = _get(conf, RuntimeArguments.catalog.stream_config)
        if value is None and RuntimeArguments.config is not None:
            value = _get(conf, RuntimeArguments.config.json_data)
        if value is None:
            value = _get(conf, dict(environ))
        if value is None:
            value = conf.default_value
        conf.validate(value)
        return value

    @classmethod
    def get_or_error(cls, key: str) -> "object":
        if (val := cls.get(key)) is not None:
            return val
        raise ConfigNotFoundError(key)
