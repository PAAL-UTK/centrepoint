# src/main.py

from centrepoint.cli.download import get_parser, run_download
from centrepoint.dwh.creator import SensorDWHBuilder
import asyncio
from pathlib import Path

def main():
    parser = get_parser()
    args = parser.parse_args()
    asyncio.run(run_download(args))

    print("📦 Download complete. Building local DuckDB databases...")
    data_root = Path("./data")
    dwh_root = Path("./dwh")
    builder = SensorDWHBuilder(data_root, dwh_root, verbose=True)

    for sensor in builder.sensor_columns:
        print(f"🧪 Building DuckDB for sensor: {sensor}")
        try:
            builder.build_sensor_db(sensor)
        except FileNotFoundError as e:
            print(f"⚠️  Skipping {sensor}: {e}")

if __name__ == "__main__":
    main()

