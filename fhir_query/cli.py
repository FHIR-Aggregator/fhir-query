import asyncio
import json
import logging
import sys
from typing import Any

import click
import pandas as pd
import requests
import yaml
from click_default_group import DefaultGroup
from fhir.resources.graphdefinition import GraphDefinition
from halo import Halo

from fhir_query import GraphDefinitionRunner, setup_logging
from fhir_query.dataframer import Dataframer
from fhir_query.visualizer import visualize_aggregation
from fhir_query.vocabulary import vocabulary_simplifier


@click.group(cls=DefaultGroup, default="main")
def cli():
    """Run FHIR GraphDefinition traversal."""
    pass


@cli.command()
@click.option("--fhir-base-url", required=True, help="Base URL of the FHIR server.")
@click.option("--graph-definition-id", help="ID of the GraphDefinition.")
@click.option("--graph-definition-file-path", help="Path to the GraphDefinition JSON file.")
@click.option("--path", required=False, default=None, help="Query to start traversal.")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to of sqlite db")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without making any changes.",
)
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.option("--log-file", default="app.log", help="Path to the log file.")
def main(
    fhir_base_url: str,
    graph_definition_id: str,
    graph_definition_file_path: str,
    path: str,
    debug: bool,
    db_path: str,
    dry_run: bool,
    log_file: str,
) -> None:
    """Run FHIR GraphDefinition traversal."""

    setup_logging(debug, log_file)

    if fhir_base_url.endswith("/"):
        fhir_base_url = fhir_base_url[:-1]

    if not graph_definition_id and not graph_definition_file_path:
        raise click.UsageError("You must provide either --graph-definition-id or --graph-definition-file-path.")

    runner = GraphDefinitionRunner(fhir_base_url, db_path, debug)

    async def run_runner() -> None:
        if graph_definition_file_path:
            with open(graph_definition_file_path, "r") as f:
                if graph_definition_file_path.endswith(".yaml") or graph_definition_file_path.endswith(".yml"):
                    graph_definition = yaml.safe_load(f)
                else:
                    graph_definition = json.load(f)
        else:
            graph_definition = await runner.fetch_graph_definition(graph_definition_id)

        _ = GraphDefinition(**graph_definition)
        click.echo(f"{_.id} is valid FHIR R5 GraphDefinition", file=sys.stderr)
        if dry_run:
            click.echo("Dry run mode enabled. Exiting.", file=sys.stderr)
            exit(0)

        logging.debug(runner.db_path)
        spinner = Halo(text=f"Running {_.id} traversal", spinner="dots", stream=sys.stderr)
        try:
            await runner.run(graph_definition, path, spinner)
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


@cli.command()
@click.option("--fhir-base-url", required=True, help="Base URL of the FHIR server.")
@click.option("--raw", is_flag=True, default=False, help="Do not create a dataframe. default=False")
@click.option("--tsv", is_flag=True, default=True, help="Render dataframe as tsv. default=True")
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.option("--log-file", default="./fq.log", help='Path to the log file. default="./fq.log"')
@click.argument("output_path", type=click.File("w"), required=False, default=sys.stdout)
def vocabulary(
    fhir_base_url: str,
    output_path: click.File,
    debug: bool,
    log_file: str,
    raw: bool,
    tsv: bool,
) -> None:
    """Retrieve Vocabulary Observation and ResearchStudy resources from the FHIR server.
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


@cli.command(name="visualize")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db")
@click.option("--output-path", default="/tmp/fhir-graph.html", help="path output html")
@click.option(
    "--ignored-edges", "-i", multiple=True, help="Edges to ignore in the visualization", default=["member", "part-of-study"]
)
def visualize(db_path: str, output_path: str, ignored_edges: list[str]) -> None:
    """Visualize the aggregation results."""
    from fhir_query import ResourceDB

    try:
        db = ResourceDB(db_path=db_path)
        visualize_aggregation(db.aggregate(ignored_edges), output_path)
        click.echo(f"Wrote: {output_path}", file=sys.stderr)
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        # raise e


@cli.command(name="summarize")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db")
def summarize(db_path: str) -> None:
    """Summarize the aggregation results."""
    from fhir_query import ResourceDB

    try:
        db = ResourceDB(db_path=db_path)
        yaml.dump(json.loads(json.dumps(db.aggregate())), sys.stdout, default_flow_style=False)

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        click.echo(f"Error: {e}", file=sys.stderr)
        # raise e


@cli.command(name="dataframe")
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


if __name__ == "__main__":
    cli()
