#!/usr/bin/env python3
"""
Load NSE Bhav Copy CSV files into PostgreSQL.

For each bhav_*.csv file found in the input directory the script:
  1. Reads the CSV with pandas
  2. Streams rows via COPY FROM STDIN into a session-scoped temp staging table
  3. Upserts from staging → bhav_copy (ON CONFLICT DO UPDATE)
  4. COMMITs (which clears the staging table automatically via ON COMMIT DELETE ROWS)

Usage:
    uv run python load_to_db.py [--input-dir DIR] [--db-url URL] [--since DATE] [--quiet]
"""

import argparse
import io
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import psycopg2


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_OUTPUT_COLS = [
    "date", "symbol", "series",
    "open", "high", "low", "close", "last_price", "prev_close",
    "volume", "turnover", "total_trades", "isin",
]

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS bhav_copy (
    date         DATE        NOT NULL,
    symbol       VARCHAR(20) NOT NULL,
    series       VARCHAR(10) NOT NULL,
    open         NUMERIC(12,2),
    high         NUMERIC(12,2),
    low          NUMERIC(12,2),
    close        NUMERIC(12,2),
    last_price   NUMERIC(12,2),
    prev_close   NUMERIC(12,2),
    volume       BIGINT,
    turnover     NUMERIC(20,2),
    total_trades INTEGER,
    isin         VARCHAR(12),
    PRIMARY KEY (date, symbol, series)
);
"""

_STAGING_DDL = """
CREATE TEMP TABLE IF NOT EXISTS bhav_staging (
    date         DATE,
    symbol       VARCHAR(20),
    series       VARCHAR(10),
    open         NUMERIC(12,2),
    high         NUMERIC(12,2),
    low          NUMERIC(12,2),
    close        NUMERIC(12,2),
    last_price   NUMERIC(12,2),
    prev_close   NUMERIC(12,2),
    volume       BIGINT,
    turnover     NUMERIC(20,2),
    total_trades INTEGER,
    isin         VARCHAR(12)
) ON COMMIT DELETE ROWS;
"""

_COPY_SQL = (
    "COPY bhav_staging "
    "(date, symbol, series, open, high, low, close, last_price, "
    "prev_close, volume, turnover, total_trades, isin) "
    "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
)

_UPSERT_SQL = """
INSERT INTO bhav_copy
    (date, symbol, series, open, high, low, close, last_price,
     prev_close, volume, turnover, total_trades, isin)
SELECT
    date, symbol, series, open, high, low, close, last_price,
    prev_close, volume, turnover, total_trades, isin
FROM bhav_staging
ON CONFLICT (date, symbol, series) DO UPDATE SET
    open         = EXCLUDED.open,
    high         = EXCLUDED.high,
    low          = EXCLUDED.low,
    close        = EXCLUDED.close,
    last_price   = EXCLUDED.last_price,
    prev_close   = EXCLUDED.prev_close,
    volume       = EXCLUDED.volume,
    turnover     = EXCLUDED.turnover,
    total_trades = EXCLUDED.total_trades,
    isin         = EXCLUDED.isin;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Cannot parse date {s!r}. Expected YYYY-MM-DD, e.g. 2025-01-31."
        )


def _file_start_date(path: Path) -> date | None:
    """Extract the start date from a bhav_*.csv filename.

    Handles:
      bhav_20250115.csv           → 2025-01-15
      bhav_20250101_to_20250331.csv → 2025-01-01
    """
    m = re.match(r"bhav_(\d{8})", path.stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def _file_end_date(path: Path) -> date | None:
    """Extract the end date from a merged bhav filename (bhav_YYYYMMDD_to_YYYYMMDD.csv).

    Returns None for single-day files.
    """
    m = re.match(r"bhav_\d{8}_to_(\d{8})", path.stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def _mask_password(url: str) -> str:
    return re.sub(r"://([^:@]+):([^@]+)@", r"://\1:***@", url)


def _load_file(cur, path: Path) -> int:
    """Stream one CSV into the staging table and upsert to bhav_copy.

    Returns the number of rows loaded.
    """
    df = pd.read_csv(path, low_memory=False)
    df.columns = df.columns.str.strip()

    # Add any missing expected columns as NaN so the schema is always complete
    for col in _OUTPUT_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[_OUTPUT_COLS]

    buf = io.StringIO()
    df.to_csv(buf, index=False, na_rep="")
    buf.seek(0)

    cur.copy_expert(_COPY_SQL, buf)
    cur.execute(_UPSERT_SQL)

    return len(df)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="load_to_db",
        description="Load NSE Bhav Copy CSV files into PostgreSQL.",
    )
    p.add_argument(
        "--input-dir", "-i",
        default="~/data/nse_bhav",
        metavar="DIR",
        help="Directory containing bhav_*.csv files (default: ~/data/nse_bhav)",
    )
    p.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        metavar="URL",
        help="Postgres connection string (default: $DATABASE_URL env var)",
    )
    p.add_argument(
        "--since",
        type=_parse_date,
        metavar="DATE",
        help="Only load files whose embedded date >= DATE (YYYY-MM-DD)",
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress per-file output",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.db_url:
        parser.error(
            "No database URL. Use --db-url or set the DATABASE_URL environment variable."
        )

    input_dir = Path(args.input_dir).expanduser()
    if not input_dir.is_dir():
        parser.error(f"Input directory does not exist: {input_dir}")

    # Collect and sort files by their embedded start date
    all_files = sorted(
        input_dir.glob("bhav_*.csv"),
        key=lambda p: (_file_start_date(p) or date.min),
    )

    if args.since:
        filtered = []
        for f in all_files:
            start = _file_start_date(f)
            if start is None:
                continue
            if start >= args.since:
                filtered.append(f)
            else:
                end = _file_end_date(f)
                if end is not None and end >= args.since:
                    print(
                        f"Warning: {f.name} spans {start} to {end} but its start date "
                        f"is before --since {args.since}; skipping. "
                        f"Re-run with --since {start} to include it.",
                        file=sys.stderr,
                    )
        all_files = filtered

    if not all_files:
        print("No matching files found.")
        sys.exit(0)

    if not args.quiet:
        print(
            f"Loading bhav_*.csv from {input_dir} → {_mask_password(args.db_url)}\n"
        )

    try:
        conn = psycopg2.connect(args.db_url)
    except psycopg2.OperationalError as exc:
        sys.exit(
            f"Error: could not connect to PostgreSQL:\n  {exc}\n"
            "Is the container running? Try: docker compose up -d"
        )
    try:
        # One-time setup: main table + session-scoped staging table
        with conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL)
                cur.execute(_STAGING_DDL)

        total_rows = 0
        error_count = 0

        for path in all_files:
            try:
                with conn:
                    with conn.cursor() as cur:
                        n = _load_file(cur, path)
                total_rows += n
                if not args.quiet:
                    print(f"  {path.name:<42} {n:>7,} rows  ✓")
            except Exception as exc:
                error_count += 1
                if not args.quiet:
                    print(f"  {path.name:<42} ERROR: {exc}")

        if not args.quiet:
            loaded = len(all_files) - error_count
            print()
            print("─" * 44)
            print(f"  Files processed : {loaded}")
            print(f"  Total rows      : {total_rows:,}")
            print(f"  Upserted        : {total_rows:,}")
            print(f"  Skipped (error) : {error_count}")
            print("─" * 44)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
