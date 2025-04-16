# src/centrepoint/cli/build_datawarehouse.py

import argparse
from pathlib import Path
from centrepoint.dwh.creator import SensorDWHBuilder

def get_parser():
    """Builds and returns the argument parser for the CLI tool.

    Returns:
        argparse.ArgumentParser: Configured parser for CLI arguments.
    """
    parser = argparse.ArgumentParser(description="Build DuckDB data warehouse from downloaded data")
    parser.add_argument("--data-root", type=Path, default=Path("data"), help="Path to the root folder containing downloaded sensor data")
    parser.add_argument("--dwh-root", type=Path, default=Path("dwh"), help="Path to output DuckDB data warehouse")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--study-id", type=int, default=1414, help="CentrePoint study ID for subject metadata")
    return parser

def main():
    """Main entrypoint for building the sensor data warehouse and subject metadata DB."""
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
