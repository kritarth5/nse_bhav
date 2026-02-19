# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the downloader
uv run python nse_bhav_copy.py <mode> [options]

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_nse_bhav_copy.py

# Run a specific test class or function
uv run pytest tests/test_nse_bhav_copy.py::TestFetch
uv run pytest tests/test_nse_bhav_copy.py::TestFetch::test_404_returns_none
```

## Architecture

The entire application lives in a single file: `nse_bhav_copy.py`. There is no package structure.

**Data flow:**
1. CLI (`_build_parser` / `main`) resolves a list of `date` objects from the user's chosen mode flag
2. `run_download` iterates over dates, calling `_fetch` for each
3. `_fetch` picks the right URL via `_url_for`, downloads a ZIP, parses the inner CSV with pandas, and calls `_normalise`
4. `_normalise` renames columns from either the legacy 13-column schema or the UDiFF 34-column schema into a single common 13-column output schema
5. Results are either saved as one CSV per day or concatenated and saved as a single merged CSV

**Key format boundary:** `_FORMAT_SWITCH = date(2024, 7, 8)`. Dates before this use the legacy URL and `_OLD_COL_MAP`; dates on or after use the UDiFF URL and `_NEW_COL_MAP`. Both paths produce the same `_OUTPUT_COLS`.

**Default output directory:** `~/data/nse_bhav`

## Testing

All tests are in `tests/test_nse_bhav_copy.py`. No real HTTP calls are made — the test file uses `unittest.mock` to mock `requests.Session` and `nse_bhav_copy._make_session`. Helper functions `_zip_bytes` and `_mock_session` create in-memory ZIPs and mock sessions respectively.

## Database

Credentials live in `.env` (gitignored). Copy `.env.example` to `.env` and set values before starting.

```bash
# Start Postgres (runs in background, data persists in a named Docker volume)
docker compose up -d

# Load all CSVs from the default directory (~/data/nse_bhav)
uv run python load_to_db.py

# Load only files on or after a given date
uv run python load_to_db.py --since 2025-01-01

# Load from a custom directory with an explicit connection string
uv run python load_to_db.py --input-dir ./my_csvs --db-url postgresql://nse:changeme@localhost:5432/nse_bhav

# Verify row count
psql $DATABASE_URL -c "SELECT count(*) FROM bhav_copy;"

# Stop Postgres (data is preserved in the pgdata volume)
docker compose down
```

The loader is idempotent — re-running it upserts rows and leaves the row count unchanged.
