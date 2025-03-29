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
    """Loads and concatenates downloaded data files for a subject from a given data category."""
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
