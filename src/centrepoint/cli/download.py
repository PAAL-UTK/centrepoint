# centrepoint/cli/download.py

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.progress import Progress

from centrepoint.auth import CentrePointAuth
from centrepoint.api.studies import StudiesAPI
from centrepoint.api.subjects import SubjectsAPI
from centrepoint.api.data_access import DataAccessAPI
from centrepoint.utils.files import download_data_file

console = Console()

def get_parser():
    parser = argparse.ArgumentParser(description="Download CentrePoint data files for a subject")
    parser.add_argument("--study-id", type=int, required=True, help="CentrePoint Study ID")
    parser.add_argument("--subject-identifier", type=str, required=True, help="Subject Identifier (not ID)")
    parser.add_argument("--data-category", type=str, required=True, help="raw-accelerometer, imu, or temperature")
    parser.add_argument("--start-date", type=lambda s: datetime.fromisoformat(s), required=True, help="Start date in ISO format (e.g. 2023-01-01)")
    parser.add_argument("--end-date", type=lambda s: datetime.fromisoformat(s), required=True, help="End date in ISO format (e.g. 2023-01-02)")
    parser.add_argument("--file-format", type=str, choices=["csv", "avro"], default="csv", help="File format (csv or avro)")
    parser.add_argument("--max-concurrency", type=int, default=5, help="Max number of concurrent downloads")
    return parser

async def run_download(args):
    auth = CentrePointAuth()
    subject_api = SubjectsAPI(auth)
    data_api = DataAccessAPI(auth)

    subjects = subject_api.list_subjects(args.study_id)
    subject = next((s for s in subjects.items if s.subjectIdentifier == args.subject_identifier), None)

    if not subject:
        console.print(f"❌ [red]Subject with identifier '{args.subject_identifier}' not found in study {args.study_id}.[/red]")
        return

    console.print(f"\n📦 Downloading data for subject [bold]{subject.subjectIdentifier}[/bold] (ID: {subject.id})")

    files = data_api.list_files(
        study_id=args.study_id,
        subject_id=subject.id,
        data_category=args.data_category,
        start_date=args.start_date,
        end_date=args.end_date,
        file_format=args.file_format,
    )

    if not files.items:
        console.print("⚠️  [yellow]No files found for given parameters.[/yellow]")
        return

    output_dir = Path(f"data/{args.data_category}")
    output_dir.mkdir(parents=True, exist_ok=True)

    table = Table(title="Data Files to Download")
    table.add_column("Date")
    table.add_column("File Name")
    table.add_column("Format")

    for f in files.items:
        table.add_row(f.date.date().isoformat(), f.fileName, f.fileFormat)

    console.print(table)

    sem = asyncio.Semaphore(args.max_concurrency)
    progress = Progress()
    task_id = progress.add_task("[cyan]Downloading...", total=len(files.items))

    is_gzipped = args.file_format == "csv"
    ext = ".csv" if is_gzipped else ".avro"

    with progress:
        tasks = []
        for f in files.items:
            filename_date_part = f.date.date().isoformat()
            output_filename = f"{args.subject_identifier}_{args.data_category}-{filename_date_part}{ext}"
            output_path = output_dir / output_filename
            tasks.append(download_data_file(f.downloadUrl, output_path, sem, progress, task_id, is_gzipped=is_gzipped))
        await asyncio.gather(*tasks)

    console.print("\n✅ [bold green]Download complete.[/bold green]")
