from pathlib import Path
import duckdb

class SensorDWHBuilder:
    def __init__(self, data_root: Path, dwh_root: Path):
        self.data_root = data_root
        self.dwh_root = dwh_root
        self.dwh_root.mkdir(parents=True, exist_ok=True)

        # Define which columns to extract per sensor type
        self.sensor_columns = {
            "imu": [
                "Timestamp",
                "SubjectId",
                "GyroscopeX",
                "GyroscopeY",
                "GyroscopeZ",
            ],
            "raw-accelerometer": [
                "Timestamp",
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

    def build_sensor_db(self, sensor: str):
        if sensor not in self.sensor_columns:
            raise ValueError(f"No column config defined for sensor '{sensor}'")

        parquet_dir = self.data_root / sensor
        parquet_files = sorted(parquet_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found for {sensor} in {parquet_dir}")

        print(f"🛠 Creating DuckDB for '{sensor}' from {len(parquet_files)} parquet files")

        db_path = self.dwh_root / f"{sensor}.duckdb"
        con = duckdb.connect(str(db_path))

        table_name = sensor.replace("-", "_")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

        def normalize(col: str) -> str:
            if col == "Timestamp":
                return "ts"
            # Add underscore before capital letters and lowercase the whole thing
            return "".join(
                ["_" + c.lower() if c.isupper() else c for c in col]
            ).lstrip("_")

        cols = self.sensor_columns[sensor]
        col_str = ", ".join(f'"{c}" AS {normalize(c)}' for c in cols)

        first_file = parquet_files[0]
        print(f"📦 Creating table from: {first_file.name}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT {col_str} FROM read_parquet('{first_file}')
        """)

        for pf in parquet_files[1:]:
            print(f"📥 Inserting: {pf.name}")
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT {col_str} FROM read_parquet('{str(pf)}')
            """)

        con.execute(f"CREATE INDEX idx_{table_name}_ts ON {table_name}(ts)")
        con.close()
        print(f"✅ Done building {sensor} DB at {db_path}\n")

