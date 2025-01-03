from click.testing import CliRunner
from fhir_query.cli import cli


def test_default_option() -> None:
    """Test default option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    print(result.output)
    output = result.output
    for _ in ["main*", "summarize", "visualize"]:
        assert _ in output, f"Expected {_} in {output}"


def test_help_option() -> None:
    """Test help option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["main", "--help"])
    output = result.output
    print(output)
    assert "Usage:" in output
    assert "--fhir-base-url" in output
    assert "--graph-definition-id" in output
    assert "--graph-definition-file-path" in output
    assert "--path" in output
    assert "--db-path" in output
    assert "--dry-run" in output
    assert "--debug" in output
    assert "--log-file" in output


def test_visualize_help() -> None:
    """Test visualizer help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["visualize", "--help"])
    output = result.output
    assert "Usage:" in output
    assert "--db-path" in output
    assert "--output-path" in output
