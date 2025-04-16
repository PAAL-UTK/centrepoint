# tests/test_processor.py

import pytest
import duckdb
import polars as pl
from centrepoint.dwh.processor import process_all_resultants
from centrepoint.dwh.creator import SensorDWHBuilder
from pathlib import Path
import tempfile
import numpy as np


@pytest.fixture
def dummy_imu_parquet(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("imu")
    n = 20
    df = pl.DataFrame({
        "Timestamp": np.arange(1, n + 1, dtype=float),
        "SampleOrder": np.arange(n),
        "SubjectId": [101] * n,
        "GyroscopeX": np.linspace(0.1, 0.3, n),
        "GyroscopeY": np.linspace(0.0, 0.2, n),
        "GyroscopeZ": np.linspace(0.5, 0.3, n),
    })
    out_file = tmp_path / "dummy_imu.parquet"
    df.write_parquet(out_file)
    return out_file


@pytest.fixture
def dummy_accel_parquet(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("accel")
    n = 20
    df = pl.DataFrame({
        "Timestamp": np.arange(1, n + 1, dtype=float),
        "SampleOrder": np.arange(n),
        "SubjectId": [101] * n,
        "X": np.random.randn(n),
        "Y": np.random.randn(n),
        "Z": np.random.randn(n),
    })
    out_file = tmp_path / "dummy_accel.parquet"
    df.write_parquet(out_file)
    return out_file


def test_process_all_resultants(dummy_imu_parquet, dummy_accel_parquet):
    with tempfile.TemporaryDirectory() as tmpdir:
        dwh_path = Path(tmpdir)

        # Prepare directory structure for builder
        imu_dir = dwh_path / "imu"
        accel_dir = dwh_path / "raw-accelerometer"
        imu_dir.mkdir(parents=True)
        accel_dir.mkdir(parents=True)

        imu_target = imu_dir / dummy_imu_parquet.name
        accel_target = accel_dir / dummy_accel_parquet.name
        imu_target.write_bytes(dummy_imu_parquet.read_bytes())
        accel_target.write_bytes(dummy_accel_parquet.read_bytes())

        # Use real builder to normalize and build DBs
        builder = SensorDWHBuilder(dwh_path, dwh_path, verbose=True)
        builder.build_sensor_db("imu")
        builder.build_sensor_db("raw-accelerometer")

        # Add subject metadata DB manually
        subject_id = 101
        meta_path = dwh_path / "subjects.duckdb"
        con_meta = duckdb.connect(str(meta_path))
        con_meta.execute("""
            CREATE TABLE subjects (subject_id BIGINT PRIMARY KEY, subject_identifier TEXT);
            INSERT INTO subjects VALUES (?, ?);
        """, [subject_id, "TEST_SUBJECT"])
        con_meta.close()

        # Run resultants
        process_all_resultants(
            imu_db_path=str(dwh_path / "imu.duckdb"),
            accel_db_path=str(dwh_path / "raw-accelerometer.duckdb"),
            subject_identifier="TEST_SUBJECT",
            overwrite=True
        )

        # Check outputs using Polars
        imu_con = duckdb.connect(str(dwh_path / "imu.duckdb"))
        filtered = imu_con.execute("SELECT * FROM filtered_imu").pl()
        assert "resultant_gyro" in filtered.columns
        assert filtered.height == 20
        imu_con.close()

        accel_con = duckdb.connect(str(dwh_path / "raw-accelerometer.duckdb"))
        resultant = accel_con.execute("SELECT * FROM accel_resultant").pl()
        assert "resultant_accel" in resultant.columns
        assert resultant.height == 20
        accel_con.close()

def test_dry_run_skips_processing(dummy_imu_parquet, dummy_accel_parquet, capsys):
    with tempfile.TemporaryDirectory() as tmpdir:
        dwh_path = Path(tmpdir)

        # Set up files
        imu_dir = dwh_path / "imu"
        accel_dir = dwh_path / "raw-accelerometer"
        imu_dir.mkdir()
        accel_dir.mkdir()
        imu_target = imu_dir / dummy_imu_parquet.name
        accel_target = accel_dir / dummy_accel_parquet.name
        imu_target.write_bytes(dummy_imu_parquet.read_bytes())
        accel_target.write_bytes(dummy_accel_parquet.read_bytes())

        # Build DBs
        builder = SensorDWHBuilder(dwh_path, dwh_path, verbose=False)
        builder.build_sensor_db("imu")
        builder.build_sensor_db("raw-accelerometer")

        # Add subject metadata
        meta_path = dwh_path / "subjects.duckdb"
        con = duckdb.connect(str(meta_path))
        con.execute("""
            CREATE TABLE subjects (subject_id BIGINT, subject_identifier TEXT);
            INSERT INTO subjects VALUES (101, 'TEST_SUBJECT')
        """)
        con.close()

        # Dry run — no writes
        process_all_resultants(
            imu_db_path=str(dwh_path / "imu.duckdb"),
            accel_db_path=str(dwh_path / "raw-accelerometer.duckdb"),
            subject_identifier="TEST_SUBJECT",
            overwrite=True,
            dry_run=True
        )

        # Ensure no rows were written
        con = duckdb.connect(str(dwh_path / "imu.duckdb"))
        result = con.execute("SELECT COUNT(*) FROM filtered_imu").fetchone()
        count = result[0] if result else 0
        assert count == 0
        con.close()

        out = capsys.readouterr().out
        assert "Would process chunk" in out or "Skipping already-processed chunk" in out

def test_missing_subject_identifier_fails(dummy_imu_parquet, dummy_accel_parquet, capsys):
    with tempfile.TemporaryDirectory() as tmpdir:
        dwh_path = Path(tmpdir)

        imu_dir = dwh_path / "imu"
        accel_dir = dwh_path / "raw-accelerometer"
        imu_dir.mkdir()
        accel_dir.mkdir()
        imu_target = imu_dir / dummy_imu_parquet.name
        accel_target = accel_dir / dummy_accel_parquet.name
        imu_target.write_bytes(dummy_imu_parquet.read_bytes())
        accel_target.write_bytes(dummy_accel_parquet.read_bytes())

        builder = SensorDWHBuilder(dwh_path, dwh_path, verbose=False)
        builder.build_sensor_db("imu")
        builder.build_sensor_db("raw-accelerometer")

        # Create the subjects table but leave it empty
        meta_path = dwh_path / "subjects.duckdb"
        con = duckdb.connect(str(meta_path))
        con.execute("""
            CREATE TABLE subjects (subject_id BIGINT PRIMARY KEY, subject_identifier TEXT);
        """)
        con.close()

        process_all_resultants(
            imu_db_path=str(dwh_path / "imu.duckdb"),
            accel_db_path=str(dwh_path / "raw-accelerometer.duckdb"),
            subject_identifier="MISSING_SUBJECT",
            overwrite=True
        )

        captured = capsys.readouterr().out
        assert "not found in metadata DB" in captured

