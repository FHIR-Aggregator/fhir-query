# tests/conftest.py
import logging
import urllib
from typing import Generator, Any

import pytest
import httpx
from httpx import Response
from pytest_httpx import HTTPXMock


@pytest.fixture
def mock_fhir_server(httpx_mock: HTTPXMock) -> Generator[HTTPXMock, Any, Any]:
    def dummy_callback(request: httpx.Request) -> Response:

        logging.warning(f"Request: {request.url.path}, {str(request.url.params)}")

        if request.url.path == "/ResearchStudy" and str(request.url.params) == "_id=123":
            return Response(
                200, json={"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "ResearchStudy", "id": "123"}}]}
            )

        if request.url.path == "/ResearchSubject":
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "entry": [
                        {"resource": {"resourceType": "ResearchSubject", "id": "123RS", "subject": {"reference": "Patient/123"}}},
                    ],
                },
            )

        if request.url.path == "/Patient/123":
            return Response(200, json={"resourceType": "Patient", "id": "123"})

        if request.url.path == "/Patient/999":
            return Response(404, json={"error": "Not found"})

        if (
            request.url.path == "/Patient"
            and str(request.url.params)
            == "_has%3AResearchSubject%3Asubject%3Astudy=ResearchStudy%2F123&_revinclude=ResearchSubject%3Asubject&_revinclude=Group%3Amember&_count=1000&_total=accurate"
        ):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Patient", "id": "1", "name": [{"family": "Smith", "given": ["John"]}]}},
                        {"resource": {"resourceType": "Patient", "id": "2", "name": [{"family": "Doe", "given": ["Jane"]}]}},
                        {"resource": {"resourceType": "Patient", "id": "3", "name": [{"family": "Brown", "given": ["Charlie"]}]}},
                    ],
                },
            )

        if request.url.path == "/Specimen" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Specimen", "id": "S1", "subject": {"reference": "Patient/1"}}},
                        {"resource": {"resourceType": "Specimen", "id": "S2", "subject": {"reference": "Patient/2"}}},
                        {"resource": {"resourceType": "Specimen", "id": "S3", "subject": {"reference": "Patient/3"}}},
                    ],
                },
            )

        if request.url.path == "/Group" and "member=Specimen" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Group", "id": "G1"}},
                    ],
                },
            )

        if request.url.path == "/DocumentReference" and "subject=Group" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "DocumentReference", "id": "DR1"}},
                    ],
                },
            )

        if request.url.path == "/DocumentReference" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "DocumentReference", "id": "DR2"}},
                    ],
                },
            )

        if request.url.path == "/Observation" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Observation", "id": "O1"}},
                    ],
                },
            )

        if request.url.path == "/Procedure" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Procedure", "id": "P1"}},
                    ],
                },
            )

        if request.url.path == "/ServiceRequest" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "ServiceRequest", "id": "SR1"}},
                    ],
                },
            )

        if request.url.path == "/ImagingStudy" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "ImagingStudy", "id": "IS1"}},
                    ],
                },
            )

        if request.url.path == "/Condition" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Condition", "id": "C1"}},
                    ],
                },
            )

        if request.url.path == "/Medication" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Medication", "id": "M1"}},
                    ],
                },
            )

        if request.url.path == "/MedicationAdministration" and "subject=Patient" in str(request.url.params):
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {
                            "resource": {
                                "resourceType": "MedicationAdministration",
                                "id": "MA1",
                                "medication": {"reference": {"reference": "Medication/M1"}},
                            }
                        },
                    ],
                },
            )

        if request.url.path == "/Medication":
            return Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Medication", "id": "M1"}},
                    ],
                },
            )
        # unexpected request
        print(request.url, str(request.url.params))
        assert False, f"Unexpected url:{request.url} path:{request.url.path}, params:{str(request.url.params)}"

    httpx_mock.add_callback(dummy_callback)

    yield httpx_mock
