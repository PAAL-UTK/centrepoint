# tests/conftest.py

import pytest
import polars as pl
from pathlib import Path
import tempfile

@pytest.fixture
def dummy_imu_parquet(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("imu")
    df = pl.DataFrame({
        "Timestamp": [1.0, 2.0, 3.0],
        "SampleOrder": [0, 1, 2],
        "SubjectId": [101, 101, 101],
        "GyroscopeX": [0.1, 0.2, 0.3],
        "GyroscopeY": [0.1, 0.0, -0.1],
        "GyroscopeZ": [0.5, 0.4, 0.3],
    })
    out_file = tmp_path / "dummy_imu.parquet"
    df.write_parquet(out_file)
    return out_file


@pytest.fixture
def dummy_accel_parquet(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("accel")
    df = pl.DataFrame({
        "Timestamp": [1.0, 2.0, 3.0],
        "SampleOrder": [0, 1, 2],
        "SubjectId": [101, 101, 101],
        "X": [1.0, 0.0, -1.0],
        "Y": [0.0, 1.0, 0.0],
        "Z": [0.0, 0.0, 1.0],
    })
    out_file = tmp_path / "dummy_accel.parquet"
    df.write_parquet(out_file)
    return out_file


@pytest.fixture
def temp_duckdb_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.duckdb"

