# src/centrepoint/utils/sensor_loader.py

from centrepoint.api.subjects import SubjectsAPI
from centrepoint.auth import CentrePointAuth
from pathlib import Path
import duckdb
from rich.console import Console
from rich.table import Table

class SensorDWHBuilder:
    def __init__(self, data_root: Path, dwh_root: Path, verbose: bool = False):
        self.data_root = data_root
        self.dwh_root = dwh_root
        self.verbose = verbose
        self.console = Console()
        self.dwh_root.mkdir(parents=True, exist_ok=True)

        self.sensor_columns = {
            "imu": [
                "Timestamp",
                "SampleOrder",
                "SubjectId",
                "GyroscopeX",
                "GyroscopeY",
                "GyroscopeZ",
            ],
            "raw-accelerometer": [
                "Timestamp",
                "SampleOrder",
                "SubjectId",
                "X",
                "Y",
                "Z",
            ],
            "temperature": [
                "Timestamp",
                "SubjectId",
                "TemperatureCelsius",
            ],
        }

    def build_subject_metadata_db(self, study_id: int):
        db_path = self.dwh_root / "subjects.duckdb"
        con = duckdb.connect(str(db_path))

        con.execute("""
            CREATE TABLE IF NOT EXISTS subjects (
                subject_id BIGINT PRIMARY KEY,
                subject_identifier TEXT
            )
        """)

        auth = CentrePointAuth()
        api = SubjectsAPI(auth)

        subjects = api.list_subjects(study_id).items

        existing_ids = set(row[0] for row in con.execute("SELECT subject_id FROM subjects").fetchall())

        inserted = 0
        for subj in subjects:
            if subj.id not in existing_ids:
                con.execute(
                    "INSERT INTO subjects (subject_id, subject_identifier) VALUES (?, ?)",
                    (subj.id, subj.subjectIdentifier)
                )
                inserted += 1

        con.close()
        if self.verbose:
            self.console.print(f"[green]✅ Inserted {inserted} new subjects into {db_path}[/green]")

    def build_sensor_db(self, sensor: str):
        if sensor not in self.sensor_columns:
            raise ValueError(f"No column config defined for sensor '{sensor}'")

        parquet_dir = self.data_root / sensor
        parquet_files = sorted(parquet_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found for {sensor} in {parquet_dir}")

        print(f"🛠 Creating or updating DuckDB for '{sensor}' from {len(parquet_files)} parquet files")

        db_path = self.dwh_root / f"{sensor}.duckdb"
        con = duckdb.connect(str(db_path))

        table_name = sensor.replace("-", "_")

        def normalize(col: str) -> str:
            if col == "Timestamp":
                return "ts"
            return "".join(["_" + c.lower() if c.isupper() else c for c in col]).lstrip("_")

        cols = self.sensor_columns[sensor]

        # Build column expressions including timestamp transformation
        col_exprs = []
        for c in cols:
            if c == "Timestamp":
                if sensor in ("imu", "raw-accelerometer"):
                    col_exprs.append("(" + c + " * 1000 + CAST(SampleOrder AS BIGINT) * 1000 / 128) AS ts")
                elif sensor == "temperature":
                    col_exprs.append("(" + c + " * 1000) AS ts")
                else:
                    col_exprs.append(f'{c} AS ts')
            else:
                col_exprs.append(f'"{c}" AS {normalize(c)}')

        col_str = ", ".join(col_exprs)

        first_file = parquet_files[0]
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT {col_str} FROM read_parquet('{first_file}') LIMIT 0
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _imported_files (
                sensor TEXT,
                filename TEXT PRIMARY KEY
            )
        """)

        total_inserted = 0
        summary_table = Table(show_header=True, header_style="bold cyan")
        summary_table.add_column("Filename")
        summary_table.add_column("Rows Inserted", justify="right")

        for pf in parquet_files:
            filename = pf.name
            exists = con.execute(f"SELECT 1 FROM _imported_files WHERE filename = ?", [filename]).fetchone()

            if exists:
                print(f"⏭️  Skipping already imported: {filename}")
                continue

            print(f"📅 Inserting: {filename}")
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT {col_str} FROM read_parquet('{str(pf)}')
            """)
            con.execute("INSERT INTO _imported_files (sensor, filename) VALUES (?, ?)", (sensor, filename))

            if self.verbose:
                result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(pf)}')").fetchone()
                if result:
                    count = result[0]
                    summary_table.add_row(filename, str(count))
                    total_inserted += count

        try:
            con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name}(ts)")
        except duckdb.CatalogException:
            print(f"⚠️  Could not create index on {table_name}.ts — column may not exist.")

        con.close()

        print(f"✅ Finished updating {sensor} DB at {db_path}")
        if self.verbose:
            self.console.print(summary_table)
            print(f"📊 Total new rows inserted into {table_name}: {total_inserted}\n")
