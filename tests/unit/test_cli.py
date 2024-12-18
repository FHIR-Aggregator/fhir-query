from click.testing import CliRunner
from fhir_query.cli import main


def test_help_option() -> None:
    """Test help option."""
    runner = CliRunner()
    result = runner.invoke(main, "--help")
    output = result.output
    assert "Usage:" in output
    assert "--fhir-base-url" in output
    assert "--graph-definition-id" in output
    assert "--graph-definition-file-path" in output
    assert "--start-resource-type" in output
    assert "--start-resource-id" in output
    assert "--db_path" in output
    assert "--dry-run" in output
    assert "--debug" in output
    assert "--log-file" in output
