from click.testing import CliRunner
from fhir_aggregator_client.cli import cli


def test_default_option() -> None:
    """Test default option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    print(result.output)
    output = result.output
    for _ in ["run", "vocabulary", "ls", "results"]:
        assert _ in output, f"Expected {_} in {output}"


def test_help_option() -> None:
    """Test help option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--help"])
    output = result.output
    print(output)
    assert "Usage:" in output
    assert "GRAPH_DEFINITION" in output
    assert "FHIR_QUERY" in output
    assert "--db-path" in output
    assert "--debug" in output
    assert "--log-file" in output
    assert "--fhir-base-url" in output


def test_visualize_help() -> None:
    """Test visualizer help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["results", "visualize", "--help"])
    output = result.output
    assert "Usage:" in output
    assert "--db-path" in output
    assert "OUTPUT_PATH" in output
