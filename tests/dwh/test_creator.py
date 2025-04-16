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

def test_unknown_sensor_raises(fake_parquet_dir):
    builder = SensorDWHBuilder(fake_parquet_dir, fake_parquet_dir)
    with pytest.raises(ValueError, match="No column config defined for sensor 'unknown'"):
        builder.build_sensor_db("unknown")


def test_no_parquet_files_raises(tmp_path):
    builder = SensorDWHBuilder(tmp_path, tmp_path)
    (tmp_path / "imu").mkdir()
    with pytest.raises(FileNotFoundError, match="No parquet files found"):
        builder.build_sensor_db("imu")
        
def test_build_subject_metadata_db(monkeypatch, tmp_path):
    inserted = []

    class FakeSubject:
        def __init__(self, id, identifier):
            self.id = id
            self.subjectIdentifier = identifier

    class FakeSubjectsAPI:
        def __init__(self, auth): pass
        def list_subjects(self, study_id):
            return type("Resp", (), {"items": [FakeSubject(1, "A"), FakeSubject(2, "B")]})()

    monkeypatch.setattr("centrepoint.dwh.creator.SubjectsAPI", FakeSubjectsAPI)
    monkeypatch.setattr("centrepoint.dwh.creator.CentrePointAuth", lambda: None)

    builder = SensorDWHBuilder(tmp_path, tmp_path, verbose=True)
    builder.build_subject_metadata_db(study_id=123)

    con = duckdb.connect(str(tmp_path / "subjects.duckdb"))
    rows = con.execute("SELECT * FROM subjects").fetchall()
    con.close()

    assert len(rows) == 2
    assert ("A" in [r[1] for r in rows])

def test_index_creation_fails(tmp_path):
    # Make bad parquet file with no Timestamp column
    df = pl.DataFrame({
        "SubjectId": [1, 2],
        "TemperatureCelsius": [36.5, 37.0],
        "TimeWrong": [1.0, 2.0],  # wrong timestamp col
    })
    sensor = "temperature"
    sensor_dir = tmp_path / sensor
    sensor_dir.mkdir()
    df.write_parquet(sensor_dir / "bad.parquet")

    class BrokenBuilder(SensorDWHBuilder):
        def __init__(self, data_root, dwh_root, verbose=False):
            super().__init__(data_root, dwh_root, verbose)
            self.sensor_columns["temperature"] = ["TimeWrong", "SubjectId", "TemperatureCelsius"]

    builder = BrokenBuilder(tmp_path, tmp_path, verbose=True)
    builder.build_sensor_db(sensor)

    db_path = tmp_path / f"{sensor}.duckdb"
    assert db_path.exists()

