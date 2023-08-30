from unittest import TestCase
from testfixtures import TempDirectory  # type: ignore
from nadi_sdk.input import (
    SourceInput,
    Config,
    Catalog,
    State,
    InputMissingRequiredConfigError,
    InputSchemaInvalidError,
    InputSchemaMissingError,
)
from nadi_sdk.config import CONFIGS, OUTPUT_FORMAT, OUTPUT_TYPE
import os


class TestInput(TestCase):
    def test_input(self):
        inp: dict[str, object] = {"nadi_sdk.arg.hello": 1}
        inp_schema: dict[str, object] = {
            "type": "object",
            "properties": {"nadi_sdk.arg.hello": {"type": "integer"}},
            "required": ["nadi_sdk.arg.hello"],
        }

        # test with dict object
        si = SourceInput(inp)
        self.assertEqual(None, si.input_path)
        self.assertEqual("input", si.input_type)
        self.assertEqual(None, si.input_schema)
        self.assertEqual(inp, si.input)
        self.assertEqual(1, si.get("nadi_sdk.arg.hello"))
        self.assertEqual(1, si.get_or_error("nadi_sdk.arg.hello"))
        self.assertEqual(None, si.get("nadi_sdk.arg.unknown"))
        self.assertRaises(
            InputMissingRequiredConfigError, si.get_or_error, "nadi_sdk.arg.unknown"
        )
        self.assertRaises(InputSchemaMissingError, si.validate_schema)
        si.input_schema = inp_schema
        self.assertEqual(None, si.validate_schema())

        # test with get from environment variables
        os.environ["nadi_sdk.arg.env"] = "2"
        self.assertEqual("2", si.get("nadi_sdk.arg.env"))
        del os.environ["nadi_sdk.arg.env"]

        # test with str object (read from file behaviour)
        with TempDirectory() as d:
            d.write("input.json", b'{"nadi_sdk.arg.hello": 1}')
            si = SourceInput(f"{d.path}/input.json")
            self.assertEqual(inp, si.input)

        with TempDirectory() as d:
            d.write(
                "input_schema.json",
                b'{"type": "object","properties": {"nadi_sdk.arg.hello": {"type": "string"}}}',
            )
            si.input_schema = f"{d.path}/input_schema.json"
            self.assertRaises(InputSchemaInvalidError, si.validate_schema)

    def test_config(self):
        inp: dict[str, object] = {
            "nadi_sdk.arg.hello": 1,
            "nadi_sdk.barg.pirate_says": "aargh",
            "nadi_sdk.argy": "argy?",
            "arg.status": "ok",
        }
        inp2: dict[str, object] = {
            CONFIGS.nadi_output_format.key: "JSON",
            CONFIGS.nadi_output_type.key: "RETURN",
        }
        c = Config(inp)

        self.assertEqual("config", c.input_type)
        self.assertEqual(1, c.get("nadi_sdk.arg.hello"))
        self.assertEqual(5, c.get(CONFIGS.nadi_stream_parallelism.key))

        # below property returns arguments with prefix removed
        # only config with prefix .arg. will be fetched
        self.assertEqual({"hello": 1}, c.arguments)
        self.assertEqual(c.output_type, OUTPUT_TYPE.STDOUT)
        self.assertEqual(c.output_format, OUTPUT_FORMAT.JSON_LINES)

        c.input = inp2
        self.assertEqual(c.output_type, OUTPUT_TYPE.RETURN)
        self.assertEqual(c.output_format, OUTPUT_FORMAT.JSON)

    def test_catalog(self):
        inp: dict[str, object] = {"nadi_sdk.arg.hello": 1}
        c = Catalog(inp)
        self.assertEqual("catalog", c.input_type)

    def test_state(self):
        inp: dict[str, object] = {"nadi_sdk.arg.hello": 1}
        c = State(inp)
        self.assertEqual("state", c.input_type)
