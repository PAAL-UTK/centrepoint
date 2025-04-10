# scripts/test_dwh_build.py

from pathlib import Path
from centrepoint.dwh.creator import SensorDWHBuilder

def main():
    data_root = Path("./data")
    dwh_root = Path("./dwh")
    builder = SensorDWHBuilder(data_root, dwh_root)

    for sensor in builder.sensor_columns:
        print(f"🧪 Building DuckDB for sensor: {sensor}")
        try:
            builder.build_sensor_db(sensor)
        except FileNotFoundError as e:
            print(f"⚠️  Skipping {sensor}: {e}")

if __name__ == "__main__":
    main()

