import subprocess
import sys

import pytest

from donald.__main__ import main


def test_cli_help_runs():
    """The CLI should not crash when invoked without arguments."""
    result = subprocess.run(
        [sys.executable, "-m", "donald", "--help"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout


def test_main_entrypoint():
    """main() delegates to click and exits with code 2 when --manager is missing."""
    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2
