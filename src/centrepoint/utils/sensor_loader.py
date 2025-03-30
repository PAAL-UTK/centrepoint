from pathlib import Path
import polars as pl
from typing import Iterator

# Define the essential columns per sensor type
COLUMNS_BY_SENSOR = {
    "raw-accelerometer": ["Timestamp", "X", "Y", "Z"],
    "imu": [
        "Timestamp",
        "GyroscopeX", "GyroscopeY", "GyroscopeZ",
        "Temperature"
    ],
    "temperature": ["Timestamp", "TemperatureCelsius"]
}

# Define the desired datatypes for each sensor's relevant columns
DTYPES_BY_SENSOR = {
    "raw-accelerometer": {
        "Timestamp": pl.Int64,
        "X": pl.Float32,
        "Y": pl.Float32,
        "Z": pl.Float32,
    },
    "imu": {
        "Timestamp": pl.Int64,
        "GyroscopeX": pl.Float32,
        "GyroscopeY": pl.Float32,
        "GyroscopeZ": pl.Float32,
        "Temperature": pl.Float32,
    },
    "temperature": {
        "Timestamp": pl.Int64,
        "TemperatureCelsius": pl.Float32,
    }
}

def get_sensor_files(sensor: str, subject_id: str, data_dir: Path = Path("data")) -> list[Path]:
    """
    Get all parquet files for a given sensor and subject.
    """
    sensor_dir = data_dir / sensor
    pattern = f"{subject_id}_{sensor}_*.parquet"
    return sorted(sensor_dir.glob(pattern))

class SensorLoader:
    def __init__(self, subject_id: str, sensor: str, data_dir: Path = Path("data")):
        self.subject_id = subject_id
        self.sensor = sensor
        self.data_dir = data_dir
        self.columns = COLUMNS_BY_SENSOR[sensor]
        self.dtypes = DTYPES_BY_SENSOR[sensor]
        self.paths = get_sensor_files(sensor, subject_id, data_dir)

    def load_lazy(self) -> pl.LazyFrame:
        """
        Load all parquet files for a sensor as a single LazyFrame, selecting only essential columns and enforcing dtypes.
        """
        lf = pl.scan_parquet([str(p) for p in self.paths]).select(self.columns)
        return lf.with_columns([pl.col(k).cast(v) for k, v in self.dtypes.items()])

    def iter_days(self) -> Iterator[pl.DataFrame]:
        """
        Yield daily DataFrames for a given sensor and subject, selecting only essential columns and enforcing dtypes.
        """
        for path in self.paths:
            df = pl.read_parquet(path, columns=self.columns)
            yield df.with_columns([pl.col(k).cast(v) for k, v in self.dtypes.items()])

    def __repr__(self):
        return f"<SensorLoader {self.sensor} for {self.subject_id} ({len(self.paths)} files)>"
