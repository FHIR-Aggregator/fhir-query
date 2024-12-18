import asyncio
import json
import logging
import sys

import click
from click_default_group import DefaultGroup
import yaml
from fhir.resources.graphdefinition import GraphDefinition
from halo import Halo

from fhir_query import GraphDefinitionRunner, setup_logging
from fhir_query.visualizer import visualize_aggregation


@click.group(cls=DefaultGroup, default="main")
def cli():
    """Run FHIR GraphDefinition traversal."""
    pass


@cli.command()
@click.option("--fhir-base-url", required=True, help="Base URL of the FHIR server.")
@click.option("--graph-definition-id", help="ID of the GraphDefinition.")
@click.option("--graph-definition-file-path", help="Path to the GraphDefinition JSON file.")
@click.option("--start-resource-type", required=True, help="ResourceType to start traversal.")
@click.option("--start-resource-id", required=True, help="ID of the starting resource.")
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
    start_resource_type: str,
    start_resource_id: str,
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

    runner = GraphDefinitionRunner(fhir_base_url, db_path)

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
        spinner.start()
        try:
            await runner.run(graph_definition, start_resource_type, start_resource_id, spinner)
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


@cli.command(name="visualize")
@click.option("--db-path", default="/tmp/fhir-graph.sqlite", help="path to sqlite db")
@click.option("--output-path", default="/tmp/fhir-graph.html", help="path output html")
def visualize(db_path: str, output_path: str) -> None:
    """Visualize the aggregation results."""
    from fhir_query import ResourceDB

    try:
        db = ResourceDB(db_path=db_path)
        visualize_aggregation(db.aggregate(), output_path)
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


if __name__ == "__main__":
    cli()
