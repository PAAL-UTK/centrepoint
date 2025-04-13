import argparse
from pathlib import Path
from centrepoint.dwh.filter_gyro import filter_gyroscope_chunks


def get_parser():
    parser = argparse.ArgumentParser(
        description="Apply lowpass filter to gyroscope data and store in filtered_imu table"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("dwh/imu.duckdb"),
        help="Path to the DuckDB file containing IMU data"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, will drop and recreate the filtered_imu table"
    )
    return parser


def run_filter(args):
    filter_gyroscope_chunks(
        db_path=args.db_path,
        overwrite=args.overwrite
    )


def run_filter_entry():
    args = get_parser().parse_args()
    run_filter(args)

if __name__ == "__main__":
    run_filter_entry()

