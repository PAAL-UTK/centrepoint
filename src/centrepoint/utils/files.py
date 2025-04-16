# centrepoint/utils/files.py

import gzip
import io
import shutil
from pathlib import Path
from asyncio import Semaphore

import httpx
import polars as pl
from rich.progress import Progress, TaskID

async def download_data_file(
    url: str,
    output_path: Path,
    sem: Semaphore,
    progress: Progress,
    task_id: TaskID,
    is_gzipped: bool = True,
):
    """Downloads a data file from a URL, with support for gzip and Avro→Parquet conversion.

    Args:
        url (str): The download URL.
        output_path (Path): Path to save the downloaded file or its converted result.
        sem (Semaphore): Semaphore to throttle concurrent downloads.
        progress (Progress): Rich progress bar object.
        task_id (TaskID): Progress task ID for updates.
        is_gzipped (bool, optional): If True, decompress gzip; otherwise treat as Avro and convert. Defaults to True.
    """
    async with sem:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()

            if is_gzipped:
                compressed = io.BytesIO(response.content)
                with gzip.GzipFile(fileobj=compressed) as gzipped:
                    with open(output_path, "wb") as out_file:
                        shutil.copyfileobj(gzipped, out_file)
            else:
                # Avro → Parquet conversion
                avro_stream = io.BytesIO(response.content)
                df = pl.read_avro(avro_stream)
                parquet_path = output_path.with_suffix(".parquet")
                df.write_parquet(parquet_path)

        progress.update(task_id, advance=1)

