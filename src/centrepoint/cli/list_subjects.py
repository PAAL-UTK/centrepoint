import argparse
import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

def get_parser():
    parser = argparse.ArgumentParser(description="List subjects from metadata DB")
    parser.add_argument("--filter", type=str, help="Substring to filter subject codes (case-insensitive)")
    return parser

def main():
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

