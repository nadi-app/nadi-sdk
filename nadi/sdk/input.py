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


class RuntimeArguments:
    config: JSONConfigInput | None = None
    catalog: JSONLinesConfigInput | None = None
    state: JSONLinesConfigInput | None = None
