from unittest import TestCase
import os

from nadi.sdk.config import *
from nadi.sdk.input import *


class TestConf(TestCase):
    def test_string_conf(self):
        string_conf = StringConf("nadi.abc.1", None)

        self.assertEqual(None, string_conf.validate("xyz"))
        self.assertEqual(None, string_conf.validate(1))
        self.assertEqual(None, string_conf.validate(True))
        self.assertRaises(ConfigValueInvalidError, string_conf.validate, None)

        string_conf = StringConf("nadi.abc.2", None, valid_values=["xyz"])
        self.assertRaises(ConfigValueInvalidError, string_conf.validate, "mnop")

        string_conf = StringConf("nadi.abc.3", None, is_required=False)
        self.assertEqual(None, string_conf.validate(None))

    def test_int_conf(self):
        int_conf = IntConf("nadi.abc.1", None)

        self.assertEqual(None, int_conf.validate(1))
        self.assertRaises(ConfigTypeInvalidError, int_conf.validate, "strew")

    def test_float_conf(self):
        float_conf = FloatConf("nadi.abc.1", None)

        self.assertEqual(None, float_conf.validate(1.23))
        self.assertRaises(ConfigTypeInvalidError, float_conf.validate, "strew")

    def test_boolean_conf(self):
        boolean_conf = BooleanConf("nadi.abc.1", None)

        self.assertEqual(None, boolean_conf.validate(True))
        self.assertEqual(None, boolean_conf.validate("False"))
        self.assertRaises(ConfigTypeInvalidError, boolean_conf.validate, 12)


class TestConfigs(TestCase):
    def setUp(self) -> None:
        Configs.supported_configs.extend(
            [
                StringConf("nadi.abc", "xyz", "abc"),
                StringConf("nadi.def", None, "def"),
                StringConf("nadi.ghi", None, "ghi", is_required=False),
            ]
        )

    def tearDown(self) -> None:
        Configs.supported_configs = Configs.supported_configs[:-3]

    def test_get_supported_conf(self):
        self.assertEqual("nadi.abc", Configs.get_supported_conf("nadi.abc").key)
        self.assertEqual("nadi.abc", Configs.get_supported_conf("abc").key)
        self.assertRaises(
            ConfigNotSupportedError, Configs.get_supported_conf, "unknown"
        )

    def test_get(self):
        self.assertEqual("xyz", Configs.get("nadi.abc"))
        self.assertEqual("xyz", Configs.get("abc"))
        self.assertRaises(ConfigNotSupportedError, Configs.get, "unknown")
        self.assertRaises(ConfigValueInvalidError, Configs.get, "nadi.def")
        self.assertRaises(ConfigValueInvalidError, Configs.get, "def")

        RuntimeArguments.config = Config({"nadi.def": "uvw_config"})
        self.assertEqual("uvw_config", Configs.get("nadi.def"))
        self.assertEqual("uvw_config", Configs.get("def"))

        RuntimeArguments.catalog = Catalog([])
        RuntimeArguments.catalog.set_stream_config({"def": "uvw_catalog"})
        self.assertEqual("uvw_catalog", Configs.get("nadi.def"))
        self.assertEqual("uvw_catalog", Configs.get("def"))

        RuntimeArguments.state = State([])
        RuntimeArguments.state.set_stream_config({"nadi.def": "uvw_state"})
        self.assertEqual("uvw_state", Configs.get("nadi.def"))
        self.assertEqual("uvw_state", Configs.get("def"))

        RuntimeArguments.state.reset_stream_config()
        self.assertEqual("uvw_catalog", Configs.get("nadi.def"))
        self.assertEqual("uvw_catalog", Configs.get("def"))

        RuntimeArguments.catalog.reset_stream_config()
        self.assertEqual("uvw_config", Configs.get("nadi.def"))
        self.assertEqual("uvw_config", Configs.get("def"))

        RuntimeArguments.config = Config({})
        self.assertRaises(ConfigValueInvalidError, Configs.get, "nadi.def")
        self.assertRaises(ConfigValueInvalidError, Configs.get, "def")

        os.environ["def"] = "uvw_env"
        self.assertEqual("uvw_env", Configs.get("nadi.def"))
        self.assertEqual("uvw_env", Configs.get("def"))

    def test_get_or_error(self):
        self.assertEqual(None, Configs.get("nadi.ghi"))
        self.assertEqual(None, Configs.get("ghi"))

        self.assertEqual("xyz", Configs.get_or_error("abc"))
        self.assertRaises(ConfigNotFoundError, Configs.get_or_error, "ghi")

    def test_to_dict(self):
        s1_conf = StringConf("nadi.abc", "xyz", "abc")
        self.assertEqual(
            {
                "key": "nadi.abc",
                "argument_key": "abc",
                "default_value": "xyz",
                "is_required": True,
                "is_secret": True,
                "valid_values": None,
                "value": "___REDACTED___",
            },
            s1_conf.to_dict(),
        )

        s2_conf = StringConf(
            "nadi.abc", "xyz", "abc", is_secret=False, valid_values=["xyz", "rst"]
        )
        self.assertEqual(
            {
                "key": "nadi.abc",
                "argument_key": "abc",
                "default_value": "xyz",
                "is_required": True,
                "is_secret": False,
                "valid_values": ["xyz", "rst"],
                "value": "xyz",
            },
            s2_conf.to_dict(),
        )

    def test_supported_config(self):
        self.assertEqual(6, len(Configs.supported_configs))
        self.assertEqual(
            None,
            Configs.add_supported_config(
                StringConf("nadi.lmno", None, "lmno", is_required=False)
            ),
        )
        self.assertEqual(None, Configs.get("nadi.lmno"))
        self.assertEqual(7, len(Configs.supported_configs))
        self.assertRaises(
            ConfigIsAlreadySupported,
            Configs.add_supported_config,
            StringConf("nadi.abc", None, "lmno", is_required=False),
        )
        self.assertEqual(7, len(Configs.supported_configs))
        self.assertRaises(
            ConfigIsAlreadySupported,
            Configs.add_supported_config,
            StringConf("nadi.lmno", None, "abc", is_required=False),
        )
        self.assertEqual(7, len(Configs.supported_configs))
