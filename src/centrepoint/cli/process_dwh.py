# src/centrepoint/cli/process_dwh.py

import argparse
from centrepoint.dwh.processor import process_all_resultants

def get_parser():
    """Builds and returns the argument parser for the resultants processing CLI.

    Returns:
        argparse.ArgumentParser: Configured parser for CLI arguments.
    """
    parser = argparse.ArgumentParser(description="Filter gyro data and compute IMU + accel resultants")
    parser.add_argument("--subject-identifier", type=str, help="Only process a specific subject by identifier (e.g. PIL_JAM_LW)")
    parser.add_argument("--imu-db", type=str, default="dwh/imu.duckdb", help="Path to IMU DuckDB file")
    parser.add_argument("--accel-db", type=str, default="dwh/raw-accelerometer.duckdb", help="Path to accelerometer DuckDB file")
    parser.add_argument("--overwrite", action="store_true", help="Drop and recreate processed tables")
    parser.add_argument("--dry-run", action="store_true", help="Print chunks to process, but do not write output")
    return parser

def main():
    """Main entrypoint for processing filtered gyro and accelerometer resultants."""
    args = get_parser().parse_args()
    process_all_resultants(
        subject_identifier=args.subject_identifier,
        imu_db_path=args.imu_db,
        accel_db_path=args.accel_db,
        overwrite=args.overwrite,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()

