from typing import Annotated
from typer import Argument, Typer, Option
from nadi.sdk.config import Configs
from nadi.sdk.input import RuntimeArguments
from nadi.sdk.source import Source


class CLI:
    app = Typer()

    fetch_app = Typer()
    app.add_typer(fetch_app, name="fetch")

    list_app = Typer()
    app.add_typer(list_app, name="list")

    source: Source | None = None

    # annotations
    ann_config = Annotated[
        str,
        Option(
            help="get application configs from this file",
            rich_help_panel="Common Inputs",
        ),
    ]
    ann_catalog = Annotated[
        str,
        Option(
            help="get stream details from this file", rich_help_panel="Common Inputs"
        ),
    ]
    ann_dry_run = Annotated[
        bool,
        Option(help="validate without actually executing", is_flag=True),
    ]
    ann_missing = Annotated[bool, Option(help="list only missing configs")]
    ann_simple = Annotated[bool, Option(help="list only basic details")]
    ann_stream_name = Annotated[
        str, Argument(help="stream name that needs to be fetched")
    ]

    @staticmethod
    def load(source: Source) -> Typer:
        CLI.source = source
        return CLI.app

    @fetch_app.command("all")
    @staticmethod
    def fetch_all(
        config: ann_config = "",
        catalog: ann_catalog = "",
        dry_run: ann_dry_run = False,
    ):
        """
        Fetch all streams defined in --catalog file.
        """
        RuntimeArguments.setup(config=config, catalog=catalog)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_all(dry_run=dry_run)

    @fetch_app.command("stream")
    @staticmethod
    def fetch_stream(
        stream: str,
        config: ann_config = "",
        dry_run: ann_dry_run = False,
    ):
        """
        Fetch stream STREAM from supported streams for application.
        """
        RuntimeArguments.setup(config=config)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_stream(stream_name=stream, dry_run=dry_run)

    @list_app.command("config")
    @staticmethod
    def list_config(
        config: ann_config = "",
        missing: ann_missing = False,
        simple: ann_simple = False,
    ):
        """
        list all configs defined for application.
        """

        def _print(conf_dict: dict[str, object]):
            if simple:
                print(f"{conf_dict['key']} [{conf_dict['argument_key']}]")
            else:
                print(conf_dict)

        RuntimeArguments.setup(config=config)
        if isinstance(CLI.source, Source):
            for conf in Configs.supported_configs:
                conf_dict = conf.to_dict()
                if missing:
                    if conf.is_required and conf_dict["value"] is None:
                        _print(conf_dict)
                else:
                    _print(conf_dict)

    @list_app.command("stream")
    @staticmethod
    def list_stream(
        config: ann_config = "",
        simple: ann_simple = False,
    ):
        """
        list all streams defined for application.
        """

        def _print(conf_dict: dict[str, object]):
            if simple:
                print(conf_dict["name"])
            else:
                print(conf_dict)

        RuntimeArguments.setup(config=config)
        if isinstance(CLI.source, Source):
            for stream in CLI.source.supported_streams:
                _print(stream.to_dict())
