#!/usr/bin/env python3
"""
NSE Bhav Copy Downloader
Downloads end-of-day (EOD) market data for all NSE-listed stocks.

Data source: NSE Archives (nsearchives.nseindia.com)

NSE changed the bhav copy format on July 8, 2024 (SEBI circular 62424).
This script handles both:
  - Legacy format  (≤ 2024-07-05): CSV with 13 columns
  - UDiFF format   (≥ 2024-07-08): CSV with 34 columns

Both are normalised to a common schema before saving.

Normalised columns
------------------
date, symbol, series, open, high, low, close, last_price, prev_close,
volume, turnover, total_trades, isin
"""

import argparse
import io
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


# ---------------------------------------------------------------------------
# URL patterns
# ---------------------------------------------------------------------------

# New UDiFF format — from 2024-07-08 onwards
_NEW_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)

# Legacy format — up to 2024-07-05
_OLD_URL = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/"
    "{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
)

# Date of format switch (first trading day with the new UDiFF format)
_FORMAT_SWITCH = date(2024, 7, 8)

# NSE began equity trading on this date
NSE_START_DATE = date(1994, 11, 3)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/zip,*/*",
    "Accept-Language": "en-IN,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

def _url_for(d: date) -> str:
    if d >= _FORMAT_SWITCH:
        return _NEW_URL.format(date=d.strftime("%Y%m%d"))
    return _OLD_URL.format(
        year=d.strftime("%Y"),
        month=d.strftime("%b").upper(),
        day=d.strftime("%d"),
    )


def _format_label(d: date) -> str:
    if d >= _FORMAT_SWITCH:
        return "UDiFF / new  (post-July 2024, 34-column CSV)"
    return "Legacy / old (pre-July 2024, 13-column CSV)"


# ---------------------------------------------------------------------------
# Column normalisation
# ---------------------------------------------------------------------------

# Old-format column → normalised name
_OLD_COL_MAP = {
    "SYMBOL":      "symbol",
    "SERIES":      "series",
    "OPEN":        "open",
    "HIGH":        "high",
    "LOW":         "low",
    "CLOSE":       "close",
    "LAST":        "last_price",
    "PREVCLOSE":   "prev_close",
    "TOTTRDQTY":   "volume",
    "TOTTRDVAL":   "turnover",
    "TIMESTAMP":   "date",
    "TOTALTRADES": "total_trades",
    "ISIN":        "isin",
}

# New (UDiFF) column → normalised name
_NEW_COL_MAP = {
    "TckrSymb":        "symbol",
    "SctySrs":         "series",
    "OpnPric":         "open",
    "HghPric":         "high",
    "LwPric":          "low",
    "ClsPric":         "close",
    "LastPric":        "last_price",
    "PrvsClsgPric":    "prev_close",
    "TtlTradgVol":     "volume",
    "TtlTrfVal":       "turnover",
    "TradDt":          "date",
    "TtlNbOfTxsExctd": "total_trades",
    "ISIN":            "isin",
}

_OUTPUT_COLS = [
    "date", "symbol", "series",
    "open", "high", "low", "close", "last_price", "prev_close",
    "volume", "turnover", "total_trades", "isin",
]


def _normalise(df: pd.DataFrame, d: date) -> pd.DataFrame:
    """Rename columns to the common schema and keep only the needed columns."""
    col_map = _NEW_COL_MAP if d >= _FORMAT_SWITCH else _OLD_COL_MAP
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    df["date"] = d.isoformat()
    present = [c for c in _OUTPUT_COLS if c in df.columns]
    return df[present]


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _fetch(
    session: requests.Session,
    d: date,
    verbose: bool,
) -> tuple[pd.DataFrame | None, str]:
    """
    Download, unzip, and parse the bhav copy for *d*.

    Returns (DataFrame, human-readable status string).
    DataFrame is None if data is unavailable for that date.
    """
    url = _url_for(d)

    if verbose:
        tqdm.write(f"  URL    : {url}")
        tqdm.write(f"  Format : {_format_label(d)}")

    try:
        t0 = time.monotonic()
        resp = session.get(url, timeout=30)
        elapsed = time.monotonic() - t0
        size_kb = len(resp.content) / 1024

        if resp.status_code != 200:
            msg = f"HTTP {resp.status_code} — skipped (holiday / weekend / not yet published)"
            if verbose:
                tqdm.write(f"  Status : {msg}")
            return None, msg

        if verbose:
            tqdm.write(f"  Status : {resp.status_code} OK  ({size_kb:.1f} KB in {elapsed:.2f}s)")

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            if verbose:
                uncompressed_kb = zf.getinfo(csv_name).file_size / 1024
                tqdm.write(f"  ZIP    : {csv_name}  ({uncompressed_kb:.1f} KB uncompressed)")
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, low_memory=False)

        df.columns = df.columns.str.strip()
        raw_rows = len(df)

        if verbose:
            tqdm.write(f"  Parsed : {raw_rows:,} raw rows from CSV")

        normalised = _normalise(df, d)
        return normalised, f"OK  ({raw_rows:,} rows)"

    except zipfile.BadZipFile:
        msg = "bad ZIP — server returned unexpected content"
        if verbose:
            tqdm.write(f"  Error  : {msg}")
        return None, msg

    except requests.Timeout:
        msg = "request timed out after 30s"
        if verbose:
            tqdm.write(f"  Error  : {msg}")
        return None, msg

    except Exception as exc:
        msg = f"unexpected error: {exc}"
        if verbose:
            tqdm.write(f"  Error  : {msg}")
        return None, msg


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _weekdays(start: date, end: date) -> list[date]:
    """Return all Mon–Fri dates in [start, end]."""
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_download(
    dates: list[date],
    output_dir: Path,
    merge: bool,
    series_filter: str | None,
    quiet: bool,
) -> None:
    """
    Download bhav copy for each date in *dates*.

    merge=True  → one combined CSV covering all dates
    merge=False → one CSV per trading day
    """
    verbose = not quiet
    session = _make_session()
    frames: list[pd.DataFrame] = []
    success = 0
    skipped_dates: list[tuple[date, str]] = []

    bar = tqdm(
        dates,
        unit="day",
        disable=quiet,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    )

    for i, d in enumerate(bar, 1):
        day_name = d.strftime("%A")

        if verbose:
            tqdm.write(f"\n[{i}/{len(dates)}] {d}  ({day_name})")

        df, status = _fetch(session, d, verbose=verbose)

        if df is None:
            skipped_dates.append((d, status))
            continue

        # Apply series filter
        if series_filter:
            before = len(df)
            df = df[df["series"] == series_filter.upper()]
            after = len(df)
            if verbose:
                tqdm.write(
                    f"  Filter : series={series_filter.upper()} "
                    f"→ {before:,} rows reduced to {after:,}"
                )

        success += 1

        if merge:
            frames.append(df)
            if verbose:
                tqdm.write(f"  Queued : {len(df):,} records for merge")
        else:
            path = output_dir / f"bhav_{d.strftime('%Y%m%d')}.csv"
            df.to_csv(path, index=False)
            if verbose:
                tqdm.write(f"  Saved  : {len(df):,} records → {path}")

    # ------------------------------------------------------------------
    # Save merged file (if applicable)
    # ------------------------------------------------------------------
    merged_path: Path | None = None
    if merge:
        if not frames:
            print("\nNo data downloaded — nothing to save.", file=sys.stderr)
            sys.exit(1)

        merged = pd.concat(frames, ignore_index=True)

        if len(dates) == 1:
            name = f"bhav_{dates[0].strftime('%Y%m%d')}.csv"
        else:
            name = (
                f"bhav_{dates[0].strftime('%Y%m%d')}"
                f"_to_{dates[-1].strftime('%Y%m%d')}.csv"
            )
        merged_path = output_dir / name
        merged.to_csv(merged_path, index=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    if not quiet:
        print("\n" + "─" * 56)
        print("  SUMMARY")
        print("─" * 56)
        print(f"  Candidate dates   : {len(dates)}")
        print(f"  Trading days got  : {success}")
        print(f"  Skipped           : {len(skipped_dates)}")

        if skipped_dates and verbose:
            print("  Skipped dates:")
            for sd, reason in skipped_dates:
                print(f"    {sd}  ({reason})")

        if merge and frames:
            all_data = pd.concat(frames, ignore_index=True)
            print(f"  Total records     : {len(all_data):,}")

            if "symbol" in all_data.columns:
                print(f"  Unique symbols    : {all_data['symbol'].nunique():,}")

            if "series" in all_data.columns:
                counts = all_data["series"].value_counts()
                breakdown = "  ".join(f"{s}={c:,}" for s, c in counts.items())
                print(f"  Series breakdown  : {breakdown}")

            if "close" in all_data.columns:
                print(
                    f"  Close price range : "
                    f"{all_data['close'].min():.2f} – {all_data['close'].max():.2f}"
                )

            if merged_path:
                file_kb = merged_path.stat().st_size / 1024
                print(f"  Output file       : {merged_path}  ({file_kb:.1f} KB)")
        elif not merge:
            print(f"  Output directory  : {output_dir.resolve()}/")

        print("─" * 56)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        f"Cannot parse date {s!r}. Expected YYYY-MM-DD, e.g. 2025-01-31."
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="nse_bhav_copy",
        description=(
            "Download NSE Bhav Copy — end-of-day market data for all listed stocks.\n"
            "Output CSV columns: date, symbol, series, open, high, low, close,\n"
            "                    last_price, prev_close, volume, turnover, total_trades, isin"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DATE SELECTION  (one required)
  --today           Today's session (published ~4–5 PM IST after market close)
  --yesterday       Previous calendar day
  --days N          Last N trading-day candidates (recent weekdays up to today)
  --date DATE       A specific date  (YYYY-MM-DD)
  --from DATE       Start of a custom date range (use with optional --to)
  --all             All data since NSE inception (1994-11-03) — very slow

EXAMPLES
  nse_bhav_copy --today
  nse_bhav_copy --yesterday --series EQ
  nse_bhav_copy --days 5 --merge
  nse_bhav_copy --days 30 --merge --output-dir ./last_30_days
  nse_bhav_copy --date 2025-01-15
  nse_bhav_copy --from 2025-01-01 --to 2025-03-31 --merge
  nse_bhav_copy --all --output-dir /data/nse --quiet

SERIES CODES (common)
  EQ   Normal equity (most liquid, default for analysis)
  BE   Book Entry  (physical share settlement)
  SM   Small & Medium Enterprises
  BL   Block deals
""",
    )

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--today",     action="store_true",
                      help="Download today's bhav copy")
    mode.add_argument("--yesterday", action="store_true",
                      help="Download the previous calendar day's bhav copy")
    mode.add_argument("--days",      type=int, metavar="N",
                      help="Last N trading-day candidates (weekdays up to today)")
    mode.add_argument("--date",      type=_parse_date, metavar="DATE",
                      help="A specific date (YYYY-MM-DD)")
    mode.add_argument("--from",      dest="from_date", type=_parse_date, metavar="DATE",
                      help="Start of a date range (combine with --to)")
    mode.add_argument("--all",       action="store_true",
                      help="All available data since NSE inception (very slow)")

    p.add_argument("--to", dest="to_date", type=_parse_date, metavar="DATE",
                   help="End date for --from range (default: today)")
    p.add_argument("--output-dir", "-o", default="~/data/nse_bhav", metavar="DIR",
                   help="Directory to save CSV files (default: ~/data/nse_bhav)")
    p.add_argument("--merge", "-m", action="store_true",
                   help="Combine all dates into a single CSV file")
    p.add_argument("--series", metavar="CODE",
                   help="Filter by trading series, e.g. EQ, BE, SM")
    p.add_argument("--quiet", "-q", action="store_true",
                   help="Suppress all output except fatal errors")
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    today = date.today()

    # ------------------------------------------------------------------
    # Resolve the list of candidate dates
    # ------------------------------------------------------------------
    if args.today:
        dates = [today]
        mode_desc = "today"

    elif args.yesterday:
        dates = [today - timedelta(days=1)]
        mode_desc = "yesterday"

    elif args.days is not None:
        if args.days < 1:
            parser.error("--days must be a positive integer")
        pool = _weekdays(today - timedelta(days=args.days * 3 + 10), today)
        dates = pool[-args.days:]
        mode_desc = f"last {args.days} trading-day candidates"

    elif args.date:
        dates = [args.date]
        mode_desc = f"specific date {args.date}"

    elif args.from_date:
        end = args.to_date if args.to_date else today
        if args.from_date > end:
            parser.error("--from date must not be later than --to date")
        dates = _weekdays(args.from_date, end)
        mode_desc = f"range {args.from_date} → {end}"

    else:  # --all
        total = len(_weekdays(NSE_START_DATE, today))
        print(
            f"This will attempt to fetch ~{total:,} files "
            f"({NSE_START_DATE} to {today}).\n"
            "It may take many hours. Files will be saved incrementally."
        )
        if input("Continue? [y/N] ").strip().lower() != "y":
            sys.exit(0)
        dates = _weekdays(NSE_START_DATE, today)
        mode_desc = f"all data since {NSE_START_DATE}"

    # ------------------------------------------------------------------
    # Setup output directory
    # ------------------------------------------------------------------
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    # For a single date --merge doesn't change the output, but we still use
    # the merge code path so the summary stats are always printed.
    merge = args.merge or len(dates) == 1

    # ------------------------------------------------------------------
    # Print plan before starting
    # ------------------------------------------------------------------
    if not args.quiet:
        print("=" * 56)
        print("  NSE Bhav Copy Downloader")
        print("=" * 56)
        print(f"  Mode              : {mode_desc}")
        if len(dates) == 1:
            print(f"  Date              : {dates[0]}  ({dates[0].strftime('%A')})")
        else:
            print(f"  Date range        : {dates[0]} → {dates[-1]}")
            print(f"  Candidate days    : {len(dates)} weekdays")
        print(f"  Output mode       : {'single merged file' if merge else 'one file per day'}")
        print(f"  Series filter     : {args.series.upper() if args.series else 'none (all series)'}")
        print(f"  Output directory  : {output_dir.resolve()}/")
        print(f"  Format note       : dates < {_FORMAT_SWITCH} use legacy URL; "
              f"≥ {_FORMAT_SWITCH} use UDiFF URL")
        print("=" * 56)
        print()

    run_download(
        dates=dates,
        output_dir=output_dir,
        merge=merge,
        series_filter=args.series,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()
