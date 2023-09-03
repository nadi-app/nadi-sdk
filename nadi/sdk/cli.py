from typing import Annotated
from typer import Typer, Option
from nadi.sdk.input import JSONConfigInput, JSONLinesConfigInput, RuntimeArguments

from nadi.sdk.source import Source


class CLI:
    app = Typer()
    fetch_app = Typer()
    discover_app = Typer()
    app.add_typer(fetch_app, name="fetch")
    app.add_typer(discover_app, name="discover")

    source: Source | None = None

    @staticmethod
    def load(source: Source) -> Typer:
        CLI.source = source
        return CLI.app

    @fetch_app.command("all")
    @staticmethod
    def fetch_all(
        config: Annotated[str, Option(help="Config to be used.")],
        catalog: Annotated[str, Option(help="Catalog to be used.")],
        dry_run: Annotated[
            bool,
            Option(
                help="Verify if all parameters has been properly set.", is_flag=True
            ),
        ] = False,
    ):
        RuntimeArguments.catalog = JSONLinesConfigInput(catalog)
        RuntimeArguments.config = JSONConfigInput(config)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_all(dry_run=dry_run)

    @fetch_app.command()
    @staticmethod
    def fetch_stream(
        stream: str,
        config: Annotated[str, Option(help="Config to be used.")],
        catalog: Annotated[str, Option(help="Catalog to be used.")],
        dry_run: Annotated[
            bool,
            Option(
                help="Verify if all parameters has been properly set.", is_flag=True
            ),
        ] = False,
    ):
        RuntimeArguments.catalog = JSONLinesConfigInput(catalog)
        RuntimeArguments.config = JSONConfigInput(config)
        if isinstance(CLI.source, Source):
            CLI.source.fetch_stream(stream_name=stream, dry_run=dry_run)
