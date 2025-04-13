from pathlib import Path
import duckdb
import numpy as np
import polars as pl
from rich.console import Console
from rich.progress import track
from scipy.signal import butter, filtfilt

console = Console()

def butter_lowpass_filter(data: np.ndarray, cutoff: float = 35.0, fs: float = 128.0, order: int = 4) -> np.ndarray:
    b, a = butter(order, cutoff / (0.5 * fs), btype='low', analog=False)
    return filtfilt(b, a, data)

def filter_gyroscope_chunks(
    db_path: Path,
    table: str = "imu",
    output_table: str = "filtered_imu",
    overwrite: bool = False,
    chunk_size_sec: int = 600,  # 10 min
    overlap_sec: int = 1,
):
    con = duckdb.connect(str(db_path))

    if overwrite:
        con.execute(f"DROP TABLE IF EXISTS {output_table}")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {output_table} AS
        SELECT * FROM {table} LIMIT 0
    """)

    # Create or update processing tracking table
    con.execute("""
        CREATE TABLE IF NOT EXISTS _processed_chunks (
            subject_id BIGINT,
            ts_start BIGINT,
            ts_end BIGINT,
            PRIMARY KEY(subject_id, ts_start, ts_end)
        )
    """)

    subject_ids = con.execute(f"SELECT DISTINCT subject_id FROM {table} ORDER BY subject_id").fetchall()
    if not subject_ids:
        console.print(f"[red]⚠️ No subjects found in {table}![/red]")
        return

    subject_ids = [row[0] for row in subject_ids if row[0] is not None]

    fs = 128.0
    chunk_size = int(chunk_size_sec * fs)
    overlap = int(overlap_sec * fs)

    for subject_id in track(subject_ids, description="Filtering subjects"):
        result = con.execute(
            f"SELECT MIN(ts), MAX(ts) FROM {table} WHERE subject_id = {subject_id}"
        ).fetchone()

        if result is None or result[0] is None:
            continue

        ts_min, ts_max = result

        for start in range(ts_min, ts_max, chunk_size - overlap):
            end = start + chunk_size

            already_processed = con.execute("""
                SELECT 1 FROM _processed_chunks
                WHERE subject_id = ? AND ts_start = ? AND ts_end = ?
            """, [subject_id, start, end]).fetchone()

            if already_processed:
                continue

            df = con.execute(f"""
                SELECT * FROM {table}
                WHERE subject_id = {subject_id}
                AND ts BETWEEN {start} AND {end}
                ORDER BY ts
            """).pl()

            if df.is_empty():
                continue

            for axis in ["gyroscope_x", "gyroscope_y", "gyroscope_z"]:
                filtered = butter_lowpass_filter(df[axis].to_numpy(), fs=fs)
                df = df.with_columns(pl.Series(axis, filtered))

            # Append to filtered table
            con.register("df", df)
            con.execute(f"INSERT INTO {output_table} SELECT * FROM df")

            # Log the chunk as processed
            con.execute("""
                INSERT INTO _processed_chunks (subject_id, ts_start, ts_end)
                VALUES (?, ?, ?)
            """, [subject_id, start, end])

    con.close()
    console.print(f"\n✅ Filtering complete. Data written to '{output_table}' table.")

