from typing import Annotated
from typer import Typer, Option
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

    @staticmethod
    def load(source: Source) -> Typer:
        CLI.source = source
        return CLI.app

    @fetch_app.command("all")
    @staticmethod
    def fetch_all(
        config: Annotated[str, Option(help="Config to be used.")] = "",
        catalog: Annotated[str, Option(help="Catalog to be used.")] = "",
        dry_run: Annotated[
            bool,
            Option(
                help="Verify if all parameters has been properly set.", is_flag=True
            ),
        ] = False,
    ):
        RuntimeArguments.setup(config=config, catalog=catalog)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_all(dry_run=dry_run)

    @fetch_app.command("stream")
    @staticmethod
    def fetch_stream(
        stream: str,
        config: Annotated[str, Option(help="Config to be used.")] = "",
        catalog: Annotated[str, Option(help="Catalog to be used.")] = "",
        dry_run: Annotated[
            bool,
            Option(
                help="Verify if all parameters has been properly set.", is_flag=True
            ),
        ] = False,
    ):
        RuntimeArguments.setup(config=config, catalog=catalog)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_stream(stream_name=stream, dry_run=dry_run)

    @list_app.command("config")
    @staticmethod
    def list_config(
        config: Annotated[str, Option(help="Config to be used.")] = "",
        missing: Annotated[bool, Option(help="Show only missing configs.")] = False,
    ):
        RuntimeArguments.setup(config=config)
        if isinstance(CLI.source, Source):
            for conf in Configs.supported_configs:
                conf_dict = conf.to_dict()
                if missing:
                    if conf.is_required and conf_dict["value"] is None:
                        print(conf_dict)
                else:
                    print(conf_dict)
