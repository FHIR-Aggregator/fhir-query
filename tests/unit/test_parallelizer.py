from collections import defaultdict

import pytest
import yaml


@pytest.fixture
def graph_definition():
    file_path = "tests/fixtures/ResearchStudyGraph.yaml"

    # Read the YAML file
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def test_parallelizer(graph_definition):
    parallelize = defaultdict(list)
    for link in graph_definition["link"]:
        parallelize[link["sourceId"]].append(link)
    expected_values = [("ResearchStudy", 2), ("Patient", 8), ("MedicationAdministration", 1), ("Specimen", 1), ("Group", 1)]
    assert all([(k, len(v)) in expected_values for k, v in parallelize.items()])
