from unittest import TestCase
import responses
from testfixtures import TempDirectory  # type: ignore
from nadi_sdk.stream import (
    RestStream,
    RestStreamResponseInvalidError,
    Stream,
    StreamCollection,
    StreamMissingRequiredArguments,
    StreamNotSupportedError,
    StreamOutputSchemaMissingError,
    StreamOutputSchemaInvalidError,
)


class TestInput(TestCase):
    def test_stream(self):
        records: list[dict[str, object]] = [
            {"a": 1, "b": {"b1": 2, "b2": 3}, "c": ["def", "ghi", "jkl"]},
            {"a": 4, "b": {"b1": 5, "b2": 6}, "c": ["rst", "uvw", "xyz"]},
        ]
        s = Stream("stream_name", "stream description")
        # assert variables
        self.assertEqual("stream_name", s.name)
        self.assertEqual("stream description", s.description)
        self.assertEqual({}, s.arguments)
        self.assertEqual(None, s.output_schema)
        self.assertEqual(None, s.filter_json_path)
        self.assertEqual({}, s.required_arguments)

        # assert functions
        self.assertEqual(None, s.fetch_records())

        # assert filtering using json path
        self.assertEqual(records, s.filter_records(records))
        s.filter_json_path = "$[*].b"
        self.assertEqual(
            [{"b1": 2, "b2": 3}, {"b1": 5, "b2": 6}], s.filter_records(records)
        )

        self.assertEqual(None, s.validate_required_arguments())

        self.assertEqual(None, s.inject_arguments({"abc": "xyz"}))
        self.assertEqual({"abc": "xyz"}, s.arguments)
        self.assertEqual(
            {
                "name": "stream_name",
                "description": "stream description",
                "passed_arguments": {"abc": "xyz"},
            },
            s.to_dict(),
        )
        self.assertEqual(
            {
                "name": "stream_name",
                "arguments": {},
            },
            s.to_catalog(),
        )
        records2: dict[str, object] = {"a": 1, "b": "abc"}
        output_schema: dict[str, object] = {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "string"}},
            "required": ["a", "b"],
        }
        s.output_schema = output_schema
        s.validate_output_schema(records2)

        with TempDirectory() as d:
            d.write(
                "output_schema.json",
                b'{"type": "object","properties": {"a": {"type": "number"}, "c": {"type": "string"}},"required": ["a", "c"]}',
            )
            s.output_schema = f"{d.path}/output_schema.json"
            self.assertRaises(
                StreamOutputSchemaInvalidError, s.validate_output_schema, records2
            )

    @responses.activate
    def test_rest_stream(self):
        responses.add(
            responses.GET,
            "http://test-api/repo/a/issues/1",
            json={"a": 1},
            status=200,
        )
        responses.add(
            responses.GET,
            "http://test-api/repo/a/issues/2",
            status=404,
        )
        responses.add(
            responses.GET,
            "http://test-api/repo/a/issues/3",
            body="abc",
            status=200,
        )

        rs = RestStream(
            "rest_stream_name",
            "rest stream description",
            "http://test-api/repo/{repo}/issues/{issue_id}",
            arguments={"issue_id": "1"},
        )
        self.assertEqual("http://test-api/repo/{repo}/issues/{issue_id}", rs.url)
        self.assertEqual({"issue_id": "1"}, rs.arguments)
        self.assertEqual({}, rs.headers)
        self.assertEqual({}, rs.params)
        self.assertEqual(None, rs.output_schema)
        self.assertEqual({"repo": "", "issue_id": "1"}, rs.required_arguments)
        self.assertEqual({}, rs.required_headers)
        self.assertEqual({}, rs.required_params)

        self.assertEqual(
            {
                "name": "rest_stream_name",
                "description": "rest stream description",
                "passed_arguments": {"issue_id": "1"},
                "passed_params": {},
                "passed_headers": {},
                "url": "http://test-api/repo/{repo}/issues/{issue_id}",
            },
            rs.to_dict(),
        )
        self.assertEqual(
            {"name": "rest_stream_name", "arguments": {"issue_id": "1", "repo": ""}},
            rs.to_catalog(),
        )

        self.assertRaises(StreamMissingRequiredArguments, rs.fetch_records)
        rs.arguments = dict(rs.arguments, **{"repo": "a"})
        self.assertRaises(StreamOutputSchemaMissingError, rs.fetch_records)
        rs.output_schema = {
            "type": "object",
            "properties": {"a": {"type": "integer"}},
            "required": ["a"],
        }
        self.assertEqual({"a": 1}, rs.fetch_records())
        self.assertEqual(200, rs.previous_response.status_code)  # type: ignore
        rs.arguments = dict(rs.arguments, **{"issue_id": "2"})
        self.assertRaises(RestStreamResponseInvalidError, rs.fetch_records)
        rs.arguments = dict(rs.arguments, **{"issue_id": "3"})
        self.assertRaises(RestStreamResponseInvalidError, rs.fetch_records)
        self.assertEqual(False, rs.prepare_next_url())
        self.assertEqual(None, rs.reset())
        self.assertEqual(None, rs.previous_response)

        # assert injection methods
        self.assertEqual(None, rs.inject_headers({"x-head": "post"}))
        self.assertEqual(None, rs.inject_params({"page": "14"}))
        self.assertEqual({"x-head": "post"}, rs.headers)
        self.assertEqual({"page": "14"}, rs.params)
        self.assertEqual(None, rs.inject_headers({"x-head": "note"}))
        self.assertEqual(None, rs.inject_params({"book": "145"}))
        self.assertEqual({"x-head": "note"}, rs.headers)
        self.assertEqual({"page": "14", "book": "145"}, rs.params)

    def test_stream_collection(self):
        sc = StreamCollection()
        sc.add_stream(
            Stream("stream_name", "stream description"),
        )
        sc.add_stream(
            RestStream(
                "rest_stream_name",
                "rest stream description",
                "http://test-api/repo/{repo}/issues/{issue_id}",
                arguments={"repo": "xyz"},
            )
        )

        sc.inject_arguments({"abc": "123"})
        self.assertEqual({"abc": "123"}, sc.get_all()[0].arguments)
        self.assertEqual({"abc": "123", "repo": "xyz"}, sc.get_all()[1].arguments)

        sc.inject_params({"ingredient": "olives"})
        self.assertEqual({"ingredient": "olives"}, sc.get_all()[1].params)  # type: ignore

        sc.inject_headers({"method": "saute"})
        self.assertEqual({"method": "saute"}, sc.get_all()[1].headers)  # type: ignore

        self.assertEqual(
            "rest stream description",
            sc.get_stream_by_name("rest_stream_name").description,  # type: ignore
        )
        self.assertEqual(
            None,
            sc.get_stream_by_name("invalid"),
        )
        self.assertEqual(
            "rest stream description",
            sc.get_stream_by_name_or_error("rest_stream_name").description,
        )
        self.assertRaises(
            StreamNotSupportedError, sc.get_stream_by_name_or_error, "invalid"
        )
