# tests/unit/test_fhir_server.py
import pathlib

import httpx
import pytest
from click.testing import CliRunner

from fhir_aggregator_client.cli import cli as main
from fhir_aggregator_client.dataframer import Dataframer
from fhir_aggregator_client.visualizer import visualize_aggregation


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
            "run",
            "tests/fixtures/ResearchStudyGraph.yaml",
            "/ResearchStudy?_id=123",
            "--fhir-base-url",
            "http://testserver",
            "--db-path",
            f"{tmp_path}/fhir-query.sqlite",
            "--log-file",
            f"{tmp_path}/fhir-query.log",
            "--debug",
        ],
    )
    print(result.stderr)
    print(result.stdout)
    assert result.exit_code == 0, "CLI command failed"
    assert "research-study-graph" in result.stderr, result.stderr
    assert "database available at:" in result.stderr

    assert pathlib.Path(f"{tmp_path}/fhir-query.sqlite").exists()
    assert pathlib.Path(f"{tmp_path}/fhir-query.log").exists()

    # test the database

    db = Dataframer(f"{tmp_path}/fhir-query.sqlite")
    count_resource_types = db.count_resource_types()
    print(count_resource_types)
    assert count_resource_types == {
        "Condition": 1,
        "DocumentReference": 2,
        "Group": 1,
        "ImagingStudy": 1,
        "Medication": 1,
        "MedicationAdministration": 1,
        "Observation": 1,
        "Patient": 3,
        "Procedure": 1,
        "ResearchStudy": 1,
        "ResearchSubject": 1,
        "ServiceRequest": 1,
        "Specimen": 3,
    }

    aggregated = db.aggregate()
    aggregated_keys = sorted(aggregated.keys())
    print(aggregated_keys)
    assert aggregated_keys == [
        "Condition",
        "DocumentReference",
        "Group",
        "ImagingStudy",
        "Medication",
        "MedicationAdministration",
        "Observation",
        "Patient",
        "Procedure",
        "ResearchStudy",
        "ResearchSubject",
        "ServiceRequest",
        "Specimen",
    ]

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
