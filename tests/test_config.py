from unittest import TestCase
from nadi_sdk.config import CONFIGS, InvalidConfigType, ConfigNotDefinedError


class TestConfig(TestCase):
    def test_config(self):
        self.assertEqual("arg", CONFIGS.nadi_prefix_arg.value)
        self.assertEqual("nadi_sdk.prefix.arg", CONFIGS.nadi_prefix_arg.key)
        CONFIGS.put("nadi_sdk.horror_movie", "insidous")
        self.assertEqual("insidous", CONFIGS.get("nadi_sdk.horror_movie"))
        CONFIGS.put("nadi_sdk.horror_movie", "ring")
        self.assertEqual("ring", CONFIGS.get("nadi_sdk.horror_movie"))
        self.assertRaises(InvalidConfigType, CONFIGS.put, "nadi_sdk.horror_movie", 1)
        self.assertRaises(ConfigNotDefinedError, CONFIGS.get, "nadi_sdk.comedy_movies")
