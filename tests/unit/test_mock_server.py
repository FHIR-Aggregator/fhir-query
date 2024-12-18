# tests/unit/test_fhir_server.py
import pathlib

import httpx
import pytest
from click.testing import CliRunner

from fhir_query.cli import main
from fhir_query.dataframer import Dataframer
from fhir_query.visualizer import visualize_aggregation


@pytest.mark.usefixtures("mock_fhir_server")
def test_get_patient() -> None:
    response = httpx.get("http://testserver/Patient/123")
    assert response.status_code == 200
    assert response.json() == {"resourceType": "Patient", "id": "123"}


@pytest.mark.usefixtures("mock_fhir_server")
def test_get_nonexistent_patient() -> None:
    response = httpx.get("http://testserver/Patient/999")
    assert response.status_code == 404
    assert response.json() == {"error": "Not found"}


@pytest.mark.asyncio
@pytest.mark.usefixtures("mock_fhir_server")
@pytest.mark.httpx_mock(can_send_already_matched_responses=True)
def test_runner(tmp_path: str) -> None:
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        main,
        [
            "--fhir-base-url",
            "http://testserver",
            "--start-resource-type",
            "ResearchStudy",
            "--start-resource-id",
            "123",
            "--db-path",
            f"{tmp_path}/fhir-query.sqlite",
            "--graph-definition-file-path",
            "tests/fixtures/GraphDefinition.yaml",
            "--log-file",
            f"{tmp_path}/fhir-query.log",
            "--debug",
        ],
    )
    print(result.stderr)
    print(result.stdout)
    assert result.exit_code == 0, "CLI command failed"
    assert "Running research-study-graph traversal" in result.stderr
    assert "database available at:" in result.stderr

    assert pathlib.Path(f"{tmp_path}/fhir-query.sqlite").exists()
    assert pathlib.Path(f"{tmp_path}/fhir-query.log").exists()

    # test the database

    db = Dataframer(f"{tmp_path}/fhir-query.sqlite")
    assert db.count_resource_types() == {"Patient": 3, "Specimen": 3}

    aggregated = db.aggregate()
    assert sorted(aggregated.keys()) == ["Patient", "Specimen"]
    assert aggregated["Patient"]["count"] == 3
    assert aggregated["Specimen"]["count"] == 3
    assert aggregated["Specimen"]["references"]["Patient"]["count"] == 3

    visualize_aggregation(aggregated, f"{tmp_path}/fhir-query.html")
    assert pathlib.Path(f"{tmp_path}/fhir-query.html").exists()
    # to see the visualization, cp to tmp
    # shutil.copy(f"{tmp_path}/fhir-query.html", "/tmp/fhir-query.html")

    count = 0
    for _ in db.flattened_specimens():
        count += 1
        print(_)
        assert "patient_id" in _
    assert count == 3, "Expected 3 flattened specimens"
