import pathlib
import sqlite3


def test_vocabulary(fhir_base_urls, expected_study_identifiers, tmp_path):
    """Test search parameters."""
    from click.testing import CliRunner

    runner = CliRunner()
    from fhir_aggregator_client.cli import cli

    tsv_path = str(tmp_path / "vocabulary.tsv")
    exceptions = []
    for base_url in fhir_base_urls:
        pathlib.Path(tsv_path).unlink(missing_ok=True)

        result = runner.invoke(
            cli,
            [
                "vocabulary",
                tsv_path,
                "--fhir-base-url",
                base_url,
            ],
        )

        assert result.exit_code == 0, result.output
        row_count = 0

        expected_study_identifiers = set(expected_study_identifiers)
        actual_study_identifiers = set()
        with open(tsv_path, "r") as f:
            for line in f:
                row_count += 1
                study_identifier = line.split("\t")[0].strip()
                if study_identifier != "research_study_identifiers":
                    actual_study_identifiers.add(study_identifier)

        try:
            # assert row_count == 21847, f"Found {row_count} in {base_url}"
            assert actual_study_identifiers == expected_study_identifiers, [
                [_ for _ in actual_study_identifiers.difference(expected_study_identifiers)],
                [_ for _ in expected_study_identifiers.difference(actual_study_identifiers)],
                base_url,
            ]
        except Exception as e:
            exceptions.append(e)
        assert not exceptions, exceptions
