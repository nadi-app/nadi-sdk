from nadi.sdk.util import Util


class JSONConfigInput:
    def __init__(
        self,
        json_data: "str | dict[str, object]",
    ) -> None:
        self.load_json_data(json_data)

    def load_json_data(self, json_data: "str | dict[str, object]"):
        self.json_path = json_data if isinstance(json_data, str) else None
        self.json_data = (
            Util.read_json_file(json_data) if isinstance(json_data, str) else json_data
        )
        Util.validate_against_schema(self.json_data, {"type": "object"})

    def get(self, key: str) -> "object | None":
        return self.json_data.get(key)


class JSONLineData:
    def __init__(self, name: str, configs: "dict[str, object] | None") -> None:
        self.name = name
        self.configs = configs


class JSONLinesConfigInput:
    def __init__(
        self,
        json_data: "str | list[dict[str, object]]",
    ) -> None:
        self.load_json_lines_data(json_data)
        self.__stream_config: dict[str, object] = {}

    def load_json_lines_data(self, json_data: "str | list[dict[str, object]]"):
        self.json_path = json_data if isinstance(json_data, str) else None
        json_data = (
            Util.read_json_lines_file(json_data)
            if isinstance(json_data, str)
            else json_data
        )
        self.json_lines_data: list[JSONLineData] = []
        for line in json_data:
            Util.validate_against_schema(
                line,
                {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "configs": {"type": "object"},
                    },
                    "required": ["name"],
                },
            )
            self.json_lines_data.append(
                JSONLineData(line.get("name"), line.get("configs"))  # type: ignore
            )

    @property
    def stream_config(self) -> dict[str, object]:
        return self.__stream_config

    def set_stream_config(self, stream_config: dict[str, object]):
        self.__stream_config = stream_config

    def reset_stream_config(self):
        self.__stream_config = {}


class Config(JSONConfigInput):
    def __init__(self, json_data: str | dict[str, object]) -> None:
        super().__init__(json_data)


class Catalog(JSONLinesConfigInput):
    def __init__(self, json_data: str | list[dict[str, object]]) -> None:
        super().__init__(json_data)


class State(JSONLinesConfigInput):
    def __init__(self, json_data: str | list[dict[str, object]]) -> None:
        super().__init__(json_data)


class RuntimeArguments:
    config: Config | None = None
    catalog: Catalog | None = None
    state: State | None = None

    @staticmethod
    def setup(
        config: str | None = None,
        catalog: str | None = None,
        state: str | None = None,
    ):
        config = config if config != "" else None
        catalog = catalog if catalog != "" else None
        state = state if state != "" else None

        RuntimeArguments.config = Config(config) if config is not None else None
        RuntimeArguments.catalog = Catalog(catalog) if catalog is not None else None
        RuntimeArguments.state = State(state) if state is not None else None
