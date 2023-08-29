from unittest import TestCase
from click.testing import CliRunner
from nadi.cli import cli


class TestCLI(TestCase):
    def test_config(self):
        runner = CliRunner()
        result = runner.invoke(cli)
        self.assertEqual(0, result.exit_code)
        self.assertTrue(result.output.startswith("Usage: cli"))
        result = runner.invoke(cli, ["describe"])
        self.assertEqual(2, result.exit_code)
        self.assertTrue(result.output.startswith("Usage: cli"))
        result = runner.invoke(cli, ["describe"])
