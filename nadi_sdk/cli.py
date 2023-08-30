import click
from typing import Any, Type
from nadi_sdk.source import Source
from nadi_sdk.config import CONFIGS


def cls_fetch_defined_streams() -> Type[click.Option]:
    class StreamOptions(click.Option):
        def get_default(self, ctx: click.Context, call: bool = True):
            source = ctx.ensure_object(Source)
            self.type = click.Choice(
                [stream.name for stream in source.streams.get_all()]
            )
            return super(StreamOptions, self).get_default(ctx)

    return StreamOptions


def output_format_option(f: Any):
    def callback(ctx: click.Context, param: str, value: str):
        source = ctx.ensure_object(Source)
        source.options.config.put(
            CONFIGS.nadi_output_format.key, CONFIGS.nadi_output_format.value
        )
        return value

    return click.option(
        "-f",
        "--output-format",
        type=click.Choice(["JSON", "JSON_LINES"]),
        default="JSON_LINES",
        help="Specify the output format",
        callback=callback,
    )(f)


def output_type_option(f: Any):
    def callback(ctx: click.Context, param: str, value: str):
        source = ctx.ensure_object(Source)
        source.options.config.put(
            CONFIGS.nadi_output_type.key, CONFIGS.nadi_output_type.value
        )
        return value

    return click.option(
        "-t",
        "--output-type",
        type=click.Choice(["STDOUT"]),
        default="STDOUT",
        help="Specify the output type",
        callback=callback,
    )(f)


def config_option(f: Any):
    def callback(ctx: click.Context, param: str, value: str):
        source = ctx.ensure_object(Source)
        source.options.config = value
        source.inject_arguments_from_config()
        return value

    return click.option(
        "-c",
        "--config",
        type=click.STRING,
        help="Config to be passed for processing",
        callback=callback,
        required=True,
    )(f)


def limit_option(f: Any):
    def callback(ctx: click.Context, param: str, value: str):
        return value

    return click.option(
        "-l",
        "--limit",
        type=click.INT,
        help="Number of records to be processed per stream.",
        callback=callback,
        required=False,
    )(f)


def stream_option(f: Any):
    def callback(ctx: click.Context, param: str, value: str):
        return value

    return click.option(
        "-s",
        "--stream",
        cls=cls_fetch_defined_streams(),
        callback=callback,
        help="Name of stream that needs to be processed",
        required=False,
    )(f)


def common_options(f: Any):
    f = output_format_option(f)
    f = output_type_option(f)

    return f


@click.group()
@click.pass_context
def cli(ctx: click.Context):
    pass


@cli.command()
@stream_option
@config_option
@common_options
@click.pass_obj
def discover(obj: Source, stream: str | None, **kwargs: str):
    if stream is not None:
        obj.discover_stream(stream)
    else:
        obj.discover()


@cli.command()
@stream_option
@config_option
@common_options
@click.pass_obj
def describe(obj: Source, stream: str | None, **kwargs: str):
    if stream is not None:
        obj.describe_stream(stream)
    else:
        obj.describe()


@cli.command()
@stream_option
@config_option
@common_options
@limit_option
@click.pass_obj
def fetch(obj: Source, stream: str | None, limit: int | None, **kwargs: str):
    limit_val: int = (
        limit if limit is not None else int(CONFIGS.nadi_stream_request_limit.value)
    )
    if stream is not None:
        obj.fetch_stream(stream, request_limit=limit_val)
    else:
        obj.fetch(request_limit=limit_val)
