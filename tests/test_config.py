from unittest import TestCase
from nadi.config import CONFIGS, InvalidConfigType, ConfigNotDefinedError


class TestConfig(TestCase):
    def test_config(self):
        self.assertEqual("arg", CONFIGS.nadi_prefix_arg.value)
        self.assertEqual("nadi.prefix.arg", CONFIGS.nadi_prefix_arg.key)
        CONFIGS.put("nadi.horror_movie", "insidous")
        self.assertEqual("insidous", CONFIGS.get("nadi.horror_movie"))
        CONFIGS.put("nadi.horror_movie", "ring")
        self.assertEqual("ring", CONFIGS.get("nadi.horror_movie"))
        self.assertRaises(InvalidConfigType, CONFIGS.put, "nadi.horror_movie", 1)
        self.assertRaises(ConfigNotDefinedError, CONFIGS.get, "nadi.comedy_movies")
