# tests/test_cli_smoke.py

import subprocess
import sys
from pathlib import Path

cli_dir = Path("src/centrepoint/cli")


def run_help(script_name):
    result = subprocess.run(
        [sys.executable, str(cli_dir / script_name), "--help"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    assert result.returncode == 0
    assert "usage" in result.stdout.lower()


def test_download_data_help():
    run_help("download_data.py")

def test_build_datawarehouse_help():
    run_help("build_datawarehouse.py")

def test_process_dwh_help():
    run_help("process_dwh.py")

def test_list_subjects_help():
    run_help("list_subjects.py")

