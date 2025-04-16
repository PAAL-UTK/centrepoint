# centrepoint/cli/list_subjects.py

import argparse
import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

def get_parser():
    """Builds and returns the argument parser for subject listing CLI.

    Returns:
        argparse.ArgumentParser: Configured parser for CLI arguments.
    """
    parser = argparse.ArgumentParser(description="List subjects from metadata DB")
    parser.add_argument("--filter", type=str, help="Substring to filter subject codes (case-insensitive)")
    return parser

def main():
    """Main entrypoint to list subjects stored in the local DuckDB metadata database."""
    args = get_parser().parse_args()
    db_path = Path("dwh/subjects.duckdb")
    con = duckdb.connect(str(db_path))

    sql = "SELECT subject_id, subject_identifier FROM subjects"
    if args.filter:
        sql += f" WHERE lower(subject_identifier) LIKE lower('%{args.filter}%')"
    sql += " ORDER BY subject_identifier"

    rows = con.execute(sql).fetchall()
    con.close()

    table = Table(title="Subjects in Metadata DB")
    table.add_column("Subject ID", justify="right")
    table.add_column("Subject Code")

    for sid, code in rows:
        table.add_row(str(sid), code)

    console = Console()
    console.print(table)

if __name__ == "__main__":
    main()
