# NSE Bhav Copy Downloader

Downloads end-of-day (EOD) market data for all stocks listed on the National Stock Exchange of India (NSE). Data is fetched directly from the NSE archives and saved as CSV files.

## What is a Bhav Copy?

A bhav copy is the official end-of-day price file published by NSE after market close (~4–5 PM IST). It contains OHLC prices, volume, turnover, and trade counts for every instrument traded that day.

> **Note:** NSE changed the bhav copy format on **July 8, 2024** (SEBI circular 62424), switching from a 13-column legacy CSV to a 34-column UDiFF format. This tool handles both transparently and normalises them to a consistent 13-column schema.

## Output columns

| Column | Description |
|---|---|
| `date` | Trading date (YYYY-MM-DD) |
| `symbol` | NSE ticker symbol (e.g. RELIANCE, TCS) |
| `series` | Trading series (EQ, BE, SM, …) |
| `open` | Opening price |
| `high` | Day high |
| `low` | Day low |
| `close` | Closing price |
| `last_price` | Last traded price |
| `prev_close` | Previous day's closing price |
| `volume` | Total shares traded |
| `turnover` | Total traded value (₹) |
| `total_trades` | Number of trades executed |
| `isin` | ISIN code |

## Setup

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/kritarth5/nse_bhav.git
cd nse_bhav
uv sync
```

## Usage

```bash
uv run python nse_bhav_copy.py <mode> [options]
```

### Modes (pick one)

| Flag | Description |
|---|---|
| `--today` | Today's session (data available ~4–5 PM IST after market close) |
| `--yesterday` | Previous calendar day |
| `--days N` | Last N trading-day candidates (recent weekdays) |
| `--date DATE` | A specific date (`YYYY-MM-DD`) |
| `--from DATE [--to DATE]` | A custom date range (defaults to today if `--to` is omitted) |
| `--all` | Everything since NSE inception (1994-11-03) — very slow |

### Options

| Flag | Default | Description |
|---|---|---|
| `-o, --output-dir DIR` | `~/data/nse_bhav` | Directory to save CSV files |
| `-m, --merge` | off | Combine all dates into a single CSV instead of one file per day |
| `--series CODE` | all | Filter by series code (e.g. `EQ`, `BE`, `SM`) |
| `-q, --quiet` | off | Suppress all output except fatal errors |

### Examples

```bash
# Today's data
uv run python nse_bhav_copy.py --today

# Yesterday's EQ-only data
uv run python nse_bhav_copy.py --yesterday --series EQ

# Last 5 trading days merged into one file
uv run python nse_bhav_copy.py --days 5 --merge

# Last 30 days, merged, saved to a custom directory
uv run python nse_bhav_copy.py --days 30 --merge --output-dir ./last_30_days

# A specific date
uv run python nse_bhav_copy.py --date 2025-01-15

# A date range, merged
uv run python nse_bhav_copy.py --from 2025-01-01 --to 2025-03-31 --merge

# Everything (silent, one file per day)
uv run python nse_bhav_copy.py --all --output-dir /data/nse --quiet
```

## Series codes

| Code | Description |
|---|---|
| `EQ` | Normal equity (most liquid) |
| `BE` | Book Entry (physical share settlement) |
| `SM` | Small & Medium Enterprises |
| `BL` | Block deals |

Many other series codes exist in the raw data; omitting `--series` downloads all of them.
