import asyncio
import json
import logging
import pathlib
import sys
from typing import Any

import click
import pandas as pd
import requests
import yaml
from click_default_group import DefaultGroup
from fhir.resources.graphdefinition import GraphDefinition
from halo import Halo

from fhir_aggregator_client import GraphDefinitionRunner, setup_logging, ensure_our_directory
from fhir_aggregator_client.dataframer import Dataframer
from fhir_aggregator_client.visualizer import visualize_aggregation
from fhir_aggregator_client.vocabulary import vocabulary_simplifier
from fhir_aggregator_client.graph_definition import ls as ls_graph_definitions

DEFAULT_LOG_FILE = pathlib.Path(ensure_our_directory()) / "app.log"
FHIR_BASE_ENV_VAR = "FHIR_BASE"


class CustomDefaultGroup(DefaultGroup):
    def list_commands(self, ctx):
        # def natural_keys(text):
        #     return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]
        # return sorted(self.commands.keys(), key=natural_keys)
        return ["ls", "main", "results", "vocabulary"]


@click.group(cls=CustomDefaultGroup)
def cli():
    """Run FHIR GraphDefinition traversal."""
    pass


@cli.command()
@click.option(
    "--fhir-base-url",
    required=True,
    help=f"Base URL of the FHIR server. default: env ${FHIR_BASE_ENV_VAR}",
    envvar=FHIR_BASE_ENV_VAR,
)
@click.option("--raw", is_flag=True, default=False, help="Do not create a dataframe. default=False")
@click.option("--tsv", is_flag=True, default=True, help="Render dataframe as tsv. default=True")
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.option("--log-file", default=DEFAULT_LOG_FILE, help=f"Path to the log file. default={DEFAULT_LOG_FILE}")
@click.argument("output_path", type=click.File("w"), required=False, default=sys.stdout)
def vocabulary(
    fhir_base_url: str,
    output_path: click.File,
    debug: bool,
    log_file: str,
    raw: bool,
    tsv: bool,
) -> None:
    """DataFrame of key Resources and CodeSystems.
    \b

    OUTPUT_PATH: Path to the output file. If not provided, the output will be printed to stdout.
    """

    setup_logging(debug, log_file)

    if fhir_base_url.endswith("/"):
        fhir_base_url = fhir_base_url[:-1]

    output_stream: Any = output_path

    try:
        with Halo(text="Collecting vocabularies", spinner="dots", stream=sys.stderr) as spinner:
            query_url = f"{fhir_base_url}/Observation?code=vocabulary&_include=Observation:focus"
            response = requests.get(query_url, timeout=300)
            response.raise_for_status()
            bundle = response.json()
            results = bundle
            if not raw:
                results = vocabulary_simplifier(bundle)
            if not tsv:
                yaml_results = yaml.dump(results, default_flow_style=False, sort_keys=False)
                print(yaml_results, file=output_stream)
            else:
                df = pd.DataFrame(results)
                df.to_csv(output_stream, sep="\t", index=False)
            spinner.succeed(f"Wrote {len(results)} vocabularies to {output_stream.name}")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        if debug:
            raise e


@cli.command()
def ls() -> None:
    """List all the installed GraphDefinitions."""
    for graph_description in ls_graph_definitions():
        click.echo(f"{graph_description['id']} - {graph_description['description']}")


@cli.command()
@click.option(
    "--fhir-base-url",
    required=True,
    help=f"Base URL of the FHIR server. default: env ${FHIR_BASE_ENV_VAR}",
    envvar=FHIR_BASE_ENV_VAR,
)
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to of sqlite db default: /tmp/fhir-graph.sqlite")
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.option("--log-file", default=DEFAULT_LOG_FILE, help=f"Path to the log file. default={DEFAULT_LOG_FILE}")
@click.argument("graph-definition", required=True)
@click.argument("fhir-query", required=True)
def main(
    graph_definition: str,
    fhir_query: str,
    fhir_base_url: str,
    db_path: str,
    log_file: str,
    debug: bool,
) -> None:
    """Run a GraphDefinition traversal.

    GRAPH_DEFINITION is the path|id of a GraphDefinition file.
    \nFHIR_QUERY the query to start traversal.
    """

    setup_logging(debug, log_file)

    if fhir_base_url.endswith("/"):
        fhir_base_url = fhir_base_url[:-1]

    if not graph_definition:
        raise click.UsageError("You must provide a graph_definition.")

    if not fhir_query:
        raise click.UsageError("You must provide a fhir_query.")

    if pathlib.Path(db_path).exists():
        click.secho(
            f"warning: Database already exists at {db_path} and will be used. If this is not what you intended, please remove the existing database or provide a new path.",
            file=sys.stderr,
            fg="yellow",
        )

    runner = GraphDefinitionRunner(fhir_base_url, db_path, debug)

    async def run_runner() -> None:
        graph_definitions = ls_graph_definitions()
        graph_definition_file_path = None

        # they provided a path to a file
        if pathlib.Path(graph_definition).exists():
            graph_definition_file_path = graph_definition

        # they provided an id of a graph definition
        if graph_definition in [gd["id"] for gd in graph_definitions]:
            graph_definition_file_path = [gd["path"] for gd in graph_definitions if gd["id"] == graph_definition][0]

        if graph_definition_file_path:
            with open(graph_definition_file_path, "r") as f:
                if graph_definition_file_path.endswith(".yaml") or graph_definition_file_path.endswith(".yml"):
                    graph_definition_dict = yaml.safe_load(f)
                else:
                    graph_definition_dict = json.load(f)
        else:
            # they provided an id of a graph definition on the server
            graph_definition_dict = await runner.fetch_graph_definition(graph_definition)

        _ = GraphDefinition(**graph_definition_dict)
        click.echo(f"{_.id} is valid FHIR R5 GraphDefinition", file=sys.stderr)

        logging.debug(runner.db_path)
        spinner = Halo(text=f"Running {_.id} traversal", spinner="dots", stream=sys.stderr)
        try:
            await runner.run(graph_definition_dict, fhir_query, spinner)
        finally:
            spinner.stop()
        click.echo(f"Aggregated Results: {runner.count_resource_types()}", file=sys.stderr)
        click.echo(f"database available at: {runner.db_path}", file=sys.stderr)

    try:
        asyncio.run(run_runner())
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        if debug:
            raise e


@cli.group()
def results():
    """Commands for working with the results of a GraphDefinition traversal."""
    pass


@results.command(name="visualize")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db default: /tmp/fhir-graph.sqlite")
@click.option("--output-path", default="/tmp/fhir-graph.html", help="path output html default: /tmp/fhir-graph.html")
@click.option(
    "--ignored-edges",
    "-i",
    multiple=True,
    help="Edges to ignore in the visualization default=part-of-study",
    default=["part-of-study"],
)
def visualize(db_path: str, output_path: str, ignored_edges: list[str]) -> None:
    """Visualize the FHIR Resources in the database."""
    from fhir_aggregator_client import ResourceDB

    try:
        db = ResourceDB(db_path=db_path)
        visualize_aggregation(db.aggregate(ignored_edges), output_path)
        click.echo(f"Wrote: {output_path}", file=sys.stderr)
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        # raise e


@results.command(name="summarize")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db")
def summarize(db_path: str) -> None:
    """Summarize the aggregation results."""
    from fhir_aggregator_client import ResourceDB

    try:
        db = ResourceDB(db_path=db_path)
        yaml.dump(json.loads(json.dumps(db.aggregate())), sys.stdout, default_flow_style=False)

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        # raise e


@results.command(name="dataframe")
# TODO - fix the default paths
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db")
@click.option("--output-path", default="/tmp/fhir-graph.tsv", help="path output tsv")
@click.option(
    "--dtale",
    "launch_dtale",
    default=False,
    show_default=True,
    is_flag=True,
    help="Open the graph in a browser using the dtale package for interactive data exploration.",
)
@click.argument(
    "data_type",
    required=True,
    type=click.Choice(["Specimen", "DocumentReference", "ResearchSubject", "Patient"]),
    default="Specimen",
)
def dataframe(db_path: str, output_path: str, launch_dtale: bool, data_type: str) -> None:
    """Create dataframes from the local db."""

    try:
        db = Dataframer(db_path=db_path)
        # TODO - add more data types - including condition
        assert data_type in ["Specimen", "Patient"], f"Sorry {data_type} dataframe is not supported yet."

        df: pd.DataFrame | None = None
        if data_type == "Specimen":
            df = pd.DataFrame(db.flattened_specimens())
        if data_type == "Patient":
            df = pd.DataFrame(db.flattened_patients())

        if launch_dtale:
            # TODO - add check that dtale is installed
            import dtale

            dtale.show(df, subprocess=False, open_browser=True, port=40000)
        elif df is not None:
            # export to csv
            file_name = output_path if output_path else f"{data_type}.csv"
            df.to_csv(file_name, index=False)
            click.secho(f"Saved {file_name}", file=sys.stderr)
        else:
            click.secho(f"No data found for {data_type}", file=sys.stderr)

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        # raise e


cli.set_default_command(main)

if __name__ == "__main__":
    cli()
