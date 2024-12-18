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
        if request.url.path == "/Patient/123":
            return Response(200, json={"resourceType": "Patient", "id": "123"})

        if request.url.path == "/Patient/999":
            return Response(404, json={"error": "Not found"})

        if (
            request.url.path == "/Patient"
            and str(request.url.params)
            == "_has%3AResearchSubject%3Asubject%3Astudy=ResearchStudy%2F123&_revinclude=Group%3Amember&_count=1000&_total=accurate"
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
                        {"resource": {"resourceType": "Specimen", "id": "1", "subject": {"reference": "Patient/1"}}},
                        {"resource": {"resourceType": "Specimen", "id": "2", "subject": {"reference": "Patient/2"}}},
                        {"resource": {"resourceType": "Specimen", "id": "3", "subject": {"reference": "Patient/3"}}},
                    ],
                },
            )

        # unexpected request
        print(request.url, str(request.url.params))
        assert False, f"Unexpected url {request.url.path}, {str(request.url.params)}"

    httpx_mock.add_callback(dummy_callback)

    yield httpx_mock
