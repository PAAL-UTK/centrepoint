# centrepoint/dwh/processor.py

from pathlib import Path
import duckdb
import polars as pl
import numpy as np
from scipy.signal import butter, filtfilt
from rich.console import Console
from rich.progress import track

console = Console()


def butter_lowpass_filter(data: np.ndarray, cutoff: float = 35.0, fs: float = 128.0, order: int = 4) -> np.ndarray:
    b, a = butter(order, cutoff / (0.5 * fs), btype='low')
    return filtfilt(b, a, data)


def compute_resultant(df: pl.DataFrame, x: str, y: str, z: str, output_name: str) -> pl.Series:
    return pl.Series(output_name, np.sqrt(df[x].to_numpy()**2 + df[y].to_numpy()**2 + df[z].to_numpy()**2))


def process_all_resultants(
    imu_db_path: str,
    accel_db_path: str,
    overwrite: bool = False,
    chunk_sec: int = 600,
    overlap_sec: int = 1,
    fs: float = 128.0,
    subject_identifier: str | None = None,
    dry_run: bool = False
):
    imu_con = duckdb.connect(imu_db_path)
    accel_con = duckdb.connect(accel_db_path)

    if overwrite:
        imu_con.execute("DROP TABLE IF EXISTS filtered_imu")
        accel_con.execute("DROP TABLE IF EXISTS accel_resultant")
        imu_con.execute("DROP TABLE IF EXISTS _processed_chunks")

        imu_con.execute("""
            CREATE TABLE filtered_imu (
                ts DOUBLE,
                sample_order INTEGER,
                subject_id BIGINT,
                gyroscope_x DOUBLE,
                gyroscope_y DOUBLE,
                gyroscope_z DOUBLE,
                resultant_gyro DOUBLE
            )
        """)

        accel_con.execute("""
            CREATE TABLE accel_resultant (
                ts DOUBLE,
                sample_order INTEGER,
                subject_id BIGINT,
                x DOUBLE,
                y DOUBLE,
                z DOUBLE,
                resultant_accel DOUBLE
            )
        """)

        imu_con.execute("""
            CREATE TABLE _processed_chunks (
                subject_id BIGINT,
                ts_start BIGINT,
                ts_end BIGINT,
                PRIMARY KEY(subject_id, ts_start, ts_end)
            )
        """)
    else:
        # Defensive: make sure expected tables already exist
        for con, table_name in [(imu_con, "filtered_imu"), (accel_con, "accel_resultant"), (imu_con, "_processed_chunks")]:
            result = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """).fetchone()
            exists = result[0] if result else 0
            if exists == 0:
                raise RuntimeError(f"Expected table '{table_name}' to exist. Use --overwrite to initialize.")

    # Resolve subject_id from subject_identifier
    if subject_identifier is not None:
        meta_db = str(Path(accel_db_path).parent / "subjects.duckdb")
        con_meta = duckdb.connect(meta_db)
        result = con_meta.execute(
            "SELECT subject_id FROM subjects WHERE subject_identifier = ?", (subject_identifier,)
        ).fetchone()
        con_meta.close()

        if result is None:
            console.print(f"[red]❌ Subject code '{subject_identifier}' not found in metadata DB[/red]")
            return

        subject_ids = [result[0]]
    else:
        subject_ids = imu_con.execute("SELECT DISTINCT subject_id FROM imu").fetchall()
        subject_ids = [s[0] for s in subject_ids if s[0] is not None]

    chunk_size = int(chunk_sec * fs)
    overlap = int(overlap_sec * fs)

    for subject_id in track(subject_ids, description="Processing participants"):
        result = imu_con.execute(
            f"SELECT MIN(ts), MAX(ts) FROM imu WHERE subject_id = {subject_id}"
        ).fetchone()

        if result is None or result[0] is None:
            continue

        ts_min, ts_max = map(int, result)

        for start in range(ts_min, ts_max, chunk_size - overlap):
            end = start + chunk_size

            # Skip already-processed chunks
            already_done = imu_con.execute("""
                SELECT 1 FROM _processed_chunks
                WHERE subject_id = ? AND ts_start = ? AND ts_end = ?
            """, (subject_id, start, end)).fetchone()

            if already_done:
                if dry_run:
                    console.print(f"[yellow]⏭️  Skipping already-processed chunk: subj={subject_id}, {start} → {end}[/yellow]")
                continue
            elif dry_run:
                console.print(f"[cyan]🧪 Would process chunk: subj={subject_id}, {start} → {end}[/cyan]")
                continue


            # IMU filtering + resultant
            df_imu = imu_con.execute(f"""
                SELECT * FROM imu WHERE subject_id = {subject_id}
                AND ts BETWEEN {start} AND {end} ORDER BY ts
            """).pl()

            if not df_imu.is_empty():
                for axis in ["gyroscope_x", "gyroscope_y", "gyroscope_z"]:
                    df_imu = df_imu.with_columns([
                        pl.Series(axis, butter_lowpass_filter(df_imu[axis].to_numpy(), fs=fs))
                    ])
                df_imu = df_imu.with_columns([
                    compute_resultant(df_imu, "gyroscope_x", "gyroscope_y", "gyroscope_z", "resultant_gyro")
                ])
                imu_con.register("df_imu", df_imu)
                imu_con.execute("INSERT INTO filtered_imu SELECT * FROM df_imu")

            # Accelerometer resultant
            df_accel = accel_con.execute(f"""
                SELECT * FROM raw_accelerometer WHERE subject_id = {subject_id}
                AND ts BETWEEN {start} AND {end} ORDER BY ts
            """).pl()

            if not df_accel.is_empty():
                df_accel = df_accel.with_columns([
                    compute_resultant(df_accel, "x", "y", "z", "resultant_accel")
                ])
                accel_con.register("df_accel", df_accel)
                accel_con.execute("INSERT INTO accel_resultant SELECT * FROM df_accel")

            imu_con.execute("""
                INSERT INTO _processed_chunks (subject_id, ts_start, ts_end)
                VALUES (?, ?, ?)
            """, (subject_id, start, end))

    imu_con.close()
    accel_con.close()
    console.print(f"[green]✅ Processing complete. Tables written: filtered_imu, accel_resultant[/green]")

