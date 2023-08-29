from unittest import TestCase
from nadi.config import CONFIGS, OUTPUT_TYPE
from nadi.source import Source
from nadi.stream import (
    RestStream,
    Stream,
    StreamNotSupportedError,
)

import responses


class SampleRestStream(RestStream):
    def __init__(
        self,
        name: str,
        description: str,
        url: str,
        *,
        arguments: dict[str, str] | None = None,
        output_schema: str | dict[str, object] | None = None,
        filter_json_path: str | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            name,
            description,
            url,
            arguments=arguments,
            output_schema=output_schema,
            filter_json_path=filter_json_path,
            headers=headers,
            params=params,
        )

    def prepare_next_url(self) -> bool:
        current_id = int(self.arguments["issue_id"])
        self.arguments = {"issue_id": current_id + 1}
        return current_id != 3


class TestSource(TestCase):
    @responses.activate
    def test_source(self):
        responses.add(
            responses.GET,
            "http://test-api/repo/issues/1",
            json={"a": 1},
            status=200,
        )
        responses.add(
            responses.GET,
            "http://test-api/repo/issues/2",
            json={"a": 2},
            status=200,
        )
        responses.add(
            responses.GET,
            "http://test-api/repo/issues/3",
            json=[{"a": 3}, {"a": 4}],
            status=200,
        )
        src = Source()
        src.streams.add_stream(
            Stream("stream_name", "stream description"),
        )
        src.streams.add_stream(
            SampleRestStream(
                "rest_stream_name",
                "rest stream description",
                "http://test-api/repo/issues/{issue_id}",
                output_schema={
                    "type": ["array", "object"],
                    "properties": {"a": {"type": "number"}},
                },
            )
        )
        src.options.config = {"nadi.arg.issue_id": "1"}
        src.inject_arguments_from_config()

        self.assertEqual({"issue_id": "1"}, src.options.config.arguments)
        src.options.config.put(CONFIGS.nadi_output_type.key, OUTPUT_TYPE.RETURN)
        self.assertEqual(
            OUTPUT_TYPE.RETURN,
            src.options.config.output_type,
        )
        self.assertRaises(
            StreamNotSupportedError, src.describe_stream, "invalid_stream"
        )
        self.assertEqual(
            {
                "name": "stream_name",
                "description": "stream description",
                "passed_arguments": {"issue_id": "1"},
            },
            src.describe_stream("stream_name"),
        )

        self.assertEqual(
            {"name": "rest_stream_name", "arguments": {"issue_id": "1"}},
            src.discover_stream("rest_stream_name"),
        )

        self.assertEqual(
            [
                {
                    "name": "stream_name",
                    "description": "stream description",
                    "passed_arguments": {"issue_id": "1"},
                },
                {
                    "name": "rest_stream_name",
                    "description": "rest stream description",
                    "passed_arguments": {"issue_id": "1"},
                    "url": "http://test-api/repo/issues/{issue_id}",
                    "passed_params": {},
                    "passed_headers": {},
                },
            ],
            src.describe(),
        )

        self.assertEqual(
            [
                {"name": "stream_name", "arguments": {}},
                {"name": "rest_stream_name", "arguments": {"issue_id": "1"}},
            ],
            src.discover(),
        )

        self.assertEqual(
            [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}],
            src.fetch_stream("rest_stream_name"),
        )
