import asyncio
import pytest
import gzip
import io
from pathlib import Path

import polars as pl
from centrepoint.utils.files import download_data_file
from rich.progress import Progress, TaskID

class MockProgress(Progress):
    def __init__(self):
        super().__init__()
        self.updated = False

    def update(self, task_id, advance=1, **kwargs):
        self.updated = True


@pytest.mark.asyncio
async def test_download_gzipped(tmp_path, monkeypatch):
    # Setup gzip test content
    raw_data = b"hello, gzipped world"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(raw_data)
    buf.seek(0)

    class MockResponse:
        content = buf.read()
        def raise_for_status(self): pass

    class MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url): return MockResponse()

    monkeypatch.setattr("httpx.AsyncClient", lambda: MockClient())

    progress = MockProgress()
    out_path = tmp_path / "file.txt"
    sem = asyncio.Semaphore(1)
    await download_data_file("http://fake.url", out_path, sem, progress, task_id=TaskID(1))

    assert out_path.read_bytes() == raw_data
    assert hasattr(progress, "updated")


@pytest.mark.asyncio
async def test_download_avro(tmp_path, monkeypatch):
    # Create a tiny Polars dataframe → Avro buffer
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    buf = io.BytesIO()
    df.write_avro(buf)
    buf.seek(0)

    class MockResponse:
        content = buf.read()
        def raise_for_status(self): pass

    class MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url): return MockResponse()

    monkeypatch.setattr("httpx.AsyncClient", lambda: MockClient())

    progress = MockProgress()
    out_path = tmp_path / "output.avro"
    sem = asyncio.Semaphore(1)
    await download_data_file("http://fake.url", out_path, sem, progress, task_id=TaskID(1), is_gzipped=False)

    parquet_path = out_path.with_suffix(".parquet")
    assert parquet_path.exists()

    loaded = pl.read_parquet(parquet_path)
    assert loaded.shape == (2, 2)
    assert hasattr(progress, "updated")

