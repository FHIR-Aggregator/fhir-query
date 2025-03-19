import os

import yaml
from fhir.resources.graphdefinition import GraphDefinition

from fhir_aggregator_client.graph_definition import get_installed_graph_descriptions_path


def validate_example_graph_definitions(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, "r") as stream:
                        graph_definition_dict = yaml.safe_load(stream)
                        _ = GraphDefinition(**graph_definition_dict)
                        assert _.description is not None, f"GraphDefinition description is missing in {file_path}"
                        assert _.name is not None, f"GraphDefinition name is missing in {file_path}"
                        assert _.link, f"GraphDefinition link is missing in {file_path}"
                except Exception as e:
                    raise ValueError(f"Invalid YAML file: {file_path}\nError: {e}")


def test_installed_graph_definitions():
    """Ensure the graph_definitions directory is installed and contains valid YAML files."""
    graph_descriptions_path = get_installed_graph_descriptions_path()

    # Check if the graph_descriptions directory exists
    assert os.path.isdir(graph_descriptions_path), f"graph_descriptions directory {graph_descriptions_path} is not installed"
    validate_example_graph_definitions(graph_descriptions_path)
