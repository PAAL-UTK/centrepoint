from centrepoint.cli import process_dwh
from types import SimpleNamespace
import pytest


def test_main_invokes_processor(monkeypatch):
    called = {}

    def fake_processor(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr(process_dwh, "process_all_resultants", fake_processor)

    monkeypatch.setattr(
        process_dwh, "get_parser",
        lambda: SimpleNamespace(parse_args=lambda: SimpleNamespace(
            subject_identifier="TEST_SUBJECT",
            imu_db="dummy_imu.duckdb",
            accel_db="dummy_accel.duckdb",
            overwrite=True,
            dry_run=False
        ))
    )

    process_dwh.main()

    assert called["subject_identifier"] == "TEST_SUBJECT"
    assert called["imu_db_path"] == "dummy_imu.duckdb"
    assert called["accel_db_path"] == "dummy_accel.duckdb"
    assert called["overwrite"] is True
    assert called["dry_run"] is False

