import pytest
import asyncio
from types import SimpleNamespace
from centrepoint.cli import download_data


@pytest.fixture
def mock_subject_and_files(monkeypatch):
    # Fake subject and files
    fake_subject = SimpleNamespace(id=42, subjectIdentifier="TEST_SUBJECT")
    fake_file = SimpleNamespace(
        date=(fake_date := download_data.datetime(2024, 1, 1)),
        fileName="test.avro",
        fileFormat="avro",
        downloadUrl="https://example.com/fake"
    )
    fake_files = SimpleNamespace(items=[fake_file])

    class FakeSubjectsAPI:
        def __init__(self, *args, **kwargs): pass
        def list_subjects(self, study_id): return SimpleNamespace(items=[fake_subject])

    class FakeDataAPI:
        def __init__(self, *args, **kwargs): pass
        def list_files(self, **kwargs): return fake_files

    async def fake_download_data_file(*args, **kwargs):
        return None

    monkeypatch.setattr(download_data, "SubjectsAPI", FakeSubjectsAPI)
    monkeypatch.setattr(download_data, "DataAccessAPI", FakeDataAPI)
    monkeypatch.setattr(download_data, "download_data_file", fake_download_data_file)


@pytest.mark.asyncio
async def test_run_download_single_category(mock_subject_and_files, tmp_path, monkeypatch):
    # Suppress actual console output
    monkeypatch.setattr(download_data, "console", SimpleNamespace(print=lambda *a, **k: None))

    args = SimpleNamespace(
        subject_identifier="TEST_SUBJECT",
        data_category="imu",
        start_date=download_data.datetime(2024, 1, 1),
        end_date=download_data.datetime(2024, 1, 2),
        study_id=1414,
        file_format="avro",
        max_concurrency=2
    )

    await download_data.run_download(args)

