# centrepointAPI/utils/files.py

import gzip
import io
import shutil
from pathlib import Path
from asyncio import Semaphore

import httpx
from rich.progress import Progress, TaskID


async def download_gz_file(
    url: str,
    output_path: Path,
    sem: Semaphore,
    progress: Progress,
    task_id: TaskID,
):
    async with sem:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            compressed = io.BytesIO(response.content)
            with gzip.GzipFile(fileobj=compressed) as gzipped:
                with open(output_path, "wb") as out_file:
                    shutil.copyfileobj(gzipped, out_file)

    progress.update(task_id, advance=1)

