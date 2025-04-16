# centrepoint/utils/load.py

from pathlib import Path
from typing import Literal
import polars as pl


def load_data(
    data_category: str,
    subject_identifier: str,
    file_format: Literal["csv", "avro", "parquet"] = "parquet",
    data_dir: Path = Path("data")
) -> pl.DataFrame:
    """Loads and concatenates downloaded data files for a subject from a given data category.

    Args:
        data_category (str): The type of data (e.g., 'imu', 'raw-accelerometer').
        subject_identifier (str): The subject code prefix in filenames.
        file_format (Literal): File format to load ('csv', 'avro', or 'parquet').
        data_dir (Path): Base directory where data is stored. Defaults to 'data'.

    Returns:
        pl.DataFrame: Concatenated dataset for the subject and category.

    Raises:
        FileNotFoundError: If no matching files are found.
    """
    if file_format == "avro":
        ext = ".parquet"  # Avro files are converted to Parquet on download
    else:
        ext = f".{file_format}"

    target_dir = data_dir / data_category
    files = sorted(target_dir.glob(f"{subject_identifier}_{data_category}-*{ext}"))
    if not files:
        raise FileNotFoundError(f"No {file_format.upper()} files found for {subject_identifier} in '{target_dir}'")

    dfs = []
    for file in files:
        if file_format == "csv":
            df = pl.read_csv(file)
        else:
            df = pl.read_parquet(file)
        dfs.append(df)

    return pl.concat(dfs)


def load_parquet_dir(path: Path, sort_col: str = "ts") -> pl.DataFrame:
    """Loads and concatenates all Parquet files from a directory into a single DataFrame.

    Args:
        path (Path): Directory containing .parquet files.
        sort_col (str): Column name to sort by after loading. Defaults to "ts".

    Returns:
        pl.DataFrame: A concatenated, sorted DataFrame.

    Raises:
        FileNotFoundError: If no Parquet files are found in the directory.
    """
    paths = sorted(path.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No .parquet files found in {path}")

    df = pl.concat([pl.read_parquet(p) for p in paths])
    return df.sort(sort_col)

