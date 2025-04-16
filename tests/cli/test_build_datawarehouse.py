from centrepoint.cli import build_datawarehouse
from types import SimpleNamespace


def test_main_builds_all(monkeypatch):
    calls = {"build_sensor_db": [], "build_subject_metadata_db": None}

    class FakeBuilder:
        sensor_columns = ["imu", "raw-accelerometer"]
        def __init__(self, data_root, dwh_root, verbose):
            self.data_root = data_root
            self.dwh_root = dwh_root
        def build_sensor_db(self, sensor):
            calls["build_sensor_db"].append(sensor)
        def build_subject_metadata_db(self, study_id):
            calls["build_subject_metadata_db"] = study_id

    monkeypatch.setattr(build_datawarehouse, "SensorDWHBuilder", FakeBuilder)

    monkeypatch.setattr(
        build_datawarehouse, "get_parser",
        lambda: SimpleNamespace(parse_args=lambda: SimpleNamespace(
            data_root="fake-data",
            dwh_root="fake-dwh",
            verbose=False,
            study_id=1414
        ))
    )

    build_datawarehouse.main()

    assert calls["build_sensor_db"] == ["imu", "raw-accelerometer"]
    assert calls["build_subject_metadata_db"] == 1414

