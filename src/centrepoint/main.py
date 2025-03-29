# centrepointAPI/cli.py

from centrepoint.cli.download import get_parser, run_download
import asyncio

def main():
    parser = get_parser()
    args = parser.parse_args()
    asyncio.run(run_download(args))

