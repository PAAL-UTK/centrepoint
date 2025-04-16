# tests/test_creator.py

import pytest
import duckdb
import tempfile
from pathlib import Path
import polars as pl
from centrepoint.dwh.creator import SensorDWHBuilder


@pytest.fixture
def fake_parquet_dir(tmp_path):
    data = {
        "Timestamp": [1.0, 2.0, 3.0],
        "SampleOrder": [0, 1, 2],
        "SubjectId": [123, 123, 123],
        "GyroscopeX": [0.1, 0.2, 0.3],
        "GyroscopeY": [0.0, 0.1, 0.0],
        "GyroscopeZ": [0.5, 0.4, 0.3],
    }
    df = pl.DataFrame(data)
    sensor = "imu"
    sensor_dir = tmp_path / sensor
    sensor_dir.mkdir(parents=True)
    file_path = sensor_dir / "test.parquet"
    df.write_parquet(file_path)
    return tmp_path


def test_build_sensor_db(fake_parquet_dir):
    dwh_root = tempfile.TemporaryDirectory()
    builder = SensorDWHBuilder(fake_parquet_dir, Path(dwh_root.name), verbose=True)

    builder.build_sensor_db("imu")
    db_path = Path(dwh_root.name) / "imu.duckdb"
    assert db_path.exists()

    con = duckdb.connect(str(db_path))
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = {t[0] for t in tables}

    assert "imu" in table_names
    assert "_imported_files" in table_names

    rows = con.execute("SELECT * FROM imu").fetchall()
    assert len(rows) == 3

    imported_files = con.execute("SELECT filename FROM _imported_files").fetchall()
    assert imported_files[0][0] == "test.parquet"

    con.close()

