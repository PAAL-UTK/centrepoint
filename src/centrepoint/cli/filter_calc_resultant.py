import argparse
from pathlib import Path
from centrepoint.dwh.filter_calc_resultant import filter_gyro_calc_acc_gyro_resultants


def get_parser():
    parser = argparse.ArgumentParser(
        description="Filter IMU gyroscope data and compute resultants for both IMU and accelerometer signals."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("dwh/imu.duckdb"),
        help="Path to the DuckDB file containing IMU and accelerometer data"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, will drop and recreate the filtered_imu and accel_resultant tables"
    )
    return parser


def run_resultant(args):
    filter_gyro_calc_acc_gyro_resultants(
        db_path=args.db_path,
        overwrite=args.overwrite
    )


def run_resultant_entry():
    args = get_parser().parse_args()
    run_resultant(args)


if __name__ == "__main__":
    run_resultant_entry()

