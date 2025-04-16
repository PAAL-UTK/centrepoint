# src/centrepoint/cli/build_datawarehouse.py

import argparse
from pathlib import Path
from centrepoint.dwh.creator import SensorDWHBuilder


def get_parser():
    parser = argparse.ArgumentParser(description="Build DuckDB data warehouse from downloaded data")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--dwh-root", type=Path, default=Path("dwh"))
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--study-id", type=int, default=1414, help="CentrePoint study ID for subject metadata")
    return parser

def main():
    args = get_parser().parse_args()
    builder = SensorDWHBuilder(args.data_root, args.dwh_root, verbose=args.verbose)

    for sensor in builder.sensor_columns:
        print(f"\n🧪 Building DuckDB for sensor: {sensor}")
        try:
            builder.build_sensor_db(sensor)
        except FileNotFoundError as e:
            print(f"⚠️  Skipping {sensor}: {e}")

    print("\n👥 Building subject metadata database...")
    builder.build_subject_metadata_db(args.study_id)

if __name__ == "__main__":
    main()

