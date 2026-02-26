# NSE Bhav Copy — Repository Explanation & Frontend Plan

---

## Part 1: Repository Explained

### What Is This Project?

This project is a complete **data pipeline** for NSE (National Stock Exchange of India) end-of-day stock market data. Every trading day, NSE publishes a "bhav copy" — a snapshot of every listed stock's price activity for that day. This project downloads those files, normalises them into a consistent schema, and optionally loads them into a PostgreSQL database.

The entire codebase consists of two main scripts, a test suite, and some infrastructure config. There is no package structure — everything lives at the top level.

---

### File-by-File Breakdown

#### `nse_bhav_copy.py` — The Downloader

This is the core script. It handles downloading, parsing, and saving NSE data.

**The Format Problem (Why normalisation exists)**

NSE changed its file format on **July 8, 2024** (driven by a SEBI circular). Before that date, every daily file was a 13-column CSV with column names like `SYMBOL`, `OPEN`, `TOTTRDQTY`. After that date, NSE switched to a 34-column "UDiFF" format with column names like `TckrSymb`, `OpnPric`, `TtlTradgVol`. Same data, completely different column names.

The code handles this with a single constant:
```python
_FORMAT_SWITCH = date(2024, 7, 8)
```
Any date before this uses the "legacy" column map; any date on or after uses the "UDiFF" map. Both paths produce the same 13-column output:
```
date, symbol, series, open, high, low, close, last_price, prev_close,
volume, turnover, total_trades, isin
```

**What each function does:**

- `_url_for(d)` — Given a date, returns the correct download URL from `nsearchives.nseindia.com`. The URL format itself also changed at the format switch date.
- `_make_session()` — Creates an HTTP session with browser-like headers (User-Agent, Referer) to avoid being blocked by NSE's servers.
- `_fetch(session, d)` — Downloads one day's data. Returns `(DataFrame, status_message)` or `(None, reason)` on failure. It never raises — if the download fails (404 for holidays, 429 rate-limit, timeout), it returns None with an explanation. This "soft failure" design lets the batch download loop continue even if individual days fail.
- `_normalise(df, d)` — Renames columns from whichever format the CSV uses into the common 13-column schema. Critically, it overrides the date column with the `d` parameter to prevent malformed CSVs from introducing wrong dates.
- `_weekdays(start, end)` — Filters a date range to Monday–Friday only. Used when the user requests "last N days" or a custom range — NSE doesn't publish on weekends.
- `run_download(dates, output_dir, merge, ...)` — The main loop. Iterates over a list of dates, calls `_fetch` for each, optionally filters by series (e.g. `--series EQ` to only keep equity series), and either saves one CSV per day or accumulates everything for a single merged file.
- `main()` / `_build_parser()` — The CLI. Accepts modes: `--today`, `--yesterday`, `--days N`, `--date DATE`, `--from DATE --to DATE`, `--all`. Resolves these into a list of dates, then calls `run_download`.

**Key design patterns:**
- Soft failure: `_fetch` returns None instead of raising, so one bad day doesn't stop the whole batch.
- Idempotent: Re-running for the same date just overwrites the CSV file.
- Progress bars via `tqdm` for long batches.

---

#### `load_to_db.py` — The PostgreSQL Loader

After you have downloaded CSVs, this script loads them into a PostgreSQL database. It's designed to be run repeatedly without creating duplicates.

**The database table:**
```sql
CREATE TABLE bhav_copy (
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
```

The primary key is `(date, symbol, series)` — one row per stock per trading series per day. A stock like RELIANCE can appear multiple times on the same day under different series codes (e.g. EQ for normal equity, BE for trade-for-trade segment).

**How the load works (staging table pattern):**
1. For each CSV file, pandas reads it into a DataFrame.
2. The DataFrame is serialised back to CSV in memory and streamed into a **temporary staging table** using PostgreSQL's high-performance `COPY FROM STDIN` command.
3. An `INSERT INTO bhav_copy ... SELECT FROM bhav_staging ON CONFLICT DO UPDATE` statement upserts rows from staging into the real table.
4. The transaction commits; PostgreSQL automatically clears the staging table (`ON COMMIT DELETE ROWS`).

This approach is both fast (bulk COPY is 10-100x faster than row-by-row INSERT) and idempotent (re-loading a file just overwrites existing rows with the same values).

**Key functions:**
- `_load_file(cur, path)` — Handles one CSV file end-to-end (read → stage → upsert).
- `_file_start_date(path)` / `_file_end_date(path)` — Extract dates from filenames like `bhav_20250101_to_20250331.csv` for filtering with `--since`.
- `main()` — Discovers all `bhav_*.csv` files, connects to Postgres, creates tables if needed, loads each file in a loop.

---

#### `tests/test_nse_bhav_copy.py` — Unit Tests

All tests mock HTTP calls — no real network access. The helpers `_zip_bytes()` and `_mock_session()` create in-memory ZIP files and fake `requests.Session` objects to simulate what NSE would actually return.

Test groups:
- **TestUrlFor** — URL generation is correct for dates before/after the format switch.
- **TestWeekdays** — Weekday filtering correctly excludes Saturdays and Sundays.
- **TestParseDate** — Date parsing handles all three accepted formats (YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY).
- **TestNormalise** — Column renaming works for both old and new formats; the date is always overridden.
- **TestFetch** — 404 returns None, valid ZIPs return DataFrames, corrupted ZIPs return None, timeouts return None.
- **TestTodayUnavailable** — In `--today` mode, a 404 raises a specific exception instead of silently continuing.
- **TestSeriesFilter** — `--series EQ` filtering keeps only equity rows and is case-insensitive.

---

#### `docker-compose.yml` and `.env`

`docker-compose.yml` runs a PostgreSQL 16 container with a persistent data volume. The credentials are read from `.env` (copy from `.env.example`). The database URL is:
```
postgresql://nse:changeme@localhost:5432/nse_bhav
```

#### `pyproject.toml`

Project metadata and dependencies, managed by `uv` (a fast modern Python package manager). Current dependencies: `requests` (HTTP), `pandas` (data), `tqdm` (progress bars), `psycopg2-binary` (PostgreSQL).

---

## Part 2: Frontend Plan

### What We're Building

A minimal browser-based UI that:
1. Lets you search for an NSE stock by symbol (e.g. type "REL" and see "RELIANCE" suggested)
2. Displays that stock's closing price as a line chart over a chosen date range
3. Has no build step, no npm, no React — just a Python server and a single HTML file

### Architecture Decision

```
Browser (index.html)
    │  HTTP (fetch API)
    ▼
FastAPI server (app.py)          ← new file
    │  psycopg2
    ▼
PostgreSQL bhav_copy table       ← already exists
```

**Why FastAPI?** It's modern, fast, generates interactive API docs at `/docs` automatically, and integrates cleanly with the existing Python ecosystem. The endpoints are simple enough that it adds very little overhead.

**Why a single HTML file?** The user asked for a "toy" frontend. A single `static/index.html` with inline CSS and inline JavaScript, loading Chart.js from a CDN, is deployable by just opening a browser — no build pipeline, no bundler, no framework. This is deliberately minimal.

---

### Files to Create / Modify

| File | What changes |
|------|-------------|
| `pyproject.toml` | Add `fastapi`, `uvicorn[standard]`, `python-dotenv` to dependencies |
| `app.py` | New file — the FastAPI backend |
| `static/index.html` | New file — the entire frontend |

Everything else stays the same.

---

### Step-by-Step Implementation Plan

---

#### Step 1: Add dependencies to `pyproject.toml`

**What to do:** Add three new entries to the `dependencies` list in `pyproject.toml`:

```toml
[project]
dependencies = [
    "requests>=2.28.0",
    "pandas>=1.5.0",
    "tqdm>=4.64.0",
    "psycopg2-binary>=2.9",
    "fastapi>=0.111.0",          # ← new
    "uvicorn[standard]>=0.29.0", # ← new
    "python-dotenv>=1.0.0",      # ← new
]
```

**Why `fastapi`:** The web framework. It handles routing, query parameter validation, automatic JSON serialisation, and generates API docs.

**Why `uvicorn[standard]`:** The ASGI server that runs FastAPI. The `[standard]` extra includes `watchfiles`, which enables `--reload` (the server automatically restarts when you edit files).

**Why `python-dotenv`:** Reads `.env` at startup so `DATABASE_URL` is available in `os.environ`. Without this you'd have to manually `export DATABASE_URL=...` in your shell every time.

**After adding:** Run `uv sync` to install them.

---

#### Step 2: Create the `static/` directory

```bash
mkdir static
```

FastAPI's `StaticFiles` middleware requires the directory to exist when the app starts. The `static/` folder will hold `index.html` (and could hold images, CSS files, etc. in the future).

---

#### Step 3: Create `app.py` — the FastAPI backend

Create a new file `app.py` at the project root. It will have three routes.

**Module-level setup:**
```python
import os
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()  # loads .env into os.environ
DATABASE_URL = os.environ["DATABASE_URL"]  # fails fast if not set

app = FastAPI(title="NSE Bhav Copy Viewer")
app.mount("/static", StaticFiles(directory="static"), name="static")

def _get_conn():
    return psycopg2.connect(DATABASE_URL)
```

`_get_conn()` opens a fresh connection per request. For a toy app with one user this is fine — connection overhead against a local Docker container is <5ms. Using synchronous `def` handlers (not `async def`) is correct here because psycopg2 is a synchronous library; using `async def` with blocking psycopg2 calls would freeze the entire event loop.

---

**Route 1: `GET /`**

Simply returns the HTML page:
```python
@app.get("/")
def root():
    return FileResponse("static/index.html")
```

---

**Route 2: `GET /api/symbols`**

Returns the list of all distinct stock symbols for autocomplete. Only includes `series = 'EQ'` (regular equity) since that's what most users want. There are roughly 2000 such symbols.

```python
@app.get("/api/symbols")
def get_symbols():
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM bhav_copy WHERE series = 'EQ' ORDER BY symbol"
            )
            rows = cur.fetchall()
    return {"symbols": [r[0] for r in rows]}
```

Response shape:
```json
{
  "symbols": ["AARTIIND", "ABB", "ABBOTINDIA", ..., "ZYDUSLIFE"]
}
```

~2000 strings at ~10 bytes each ≈ 20KB total. The frontend fetches this once at page load and populates the `<datalist>` element for native browser autocomplete.

---

**Route 3: `GET /api/history`**

Returns OHLCV data for a symbol over a date range.

```python
@app.get("/api/history")
def get_history(
    symbol: str = Query(...),          # required
    series: str = Query("EQ"),         # default: EQ
    from_date: date = Query(None),     # default: 1 year ago
    to_date: date = Query(None),       # default: today
):
    today = date.today()
    if to_date is None:
        to_date = today
    if from_date is None:
        from_date = today - timedelta(days=365)

    if from_date > to_date:
        raise HTTPException(status_code=400, detail="from_date must not be after to_date")

    symbol = symbol.upper().strip()
    series = series.upper().strip()

    sql = """
        SELECT date, open, high, low, close, volume
        FROM bhav_copy
        WHERE symbol = %s AND series = %s AND date >= %s AND date <= %s
        ORDER BY date ASC
    """
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (symbol, series, from_date, to_date))
            rows = cur.fetchall()

    data = [
        {
            "date": str(r["date"]),
            "open": float(r["open"]) if r["open"] is not None else None,
            "high": float(r["high"]) if r["high"] is not None else None,
            "low": float(r["low"]) if r["low"] is not None else None,
            "close": float(r["close"]) if r["close"] is not None else None,
            "volume": int(r["volume"]) if r["volume"] is not None else None,
        }
        for r in rows
    ]

    return {
        "symbol": symbol,
        "series": series,
        "from_date": str(from_date),
        "to_date": str(to_date),
        "count": len(data),
        "data": data,
    }
```

Response shape:
```json
{
  "symbol": "RELIANCE",
  "series": "EQ",
  "from_date": "2025-02-26",
  "to_date": "2026-02-26",
  "count": 243,
  "data": [
    { "date": "2025-02-26", "open": 1280.5, "high": 1295.0, "low": 1275.25, "close": 1289.75, "volume": 8432100 },
    ...
  ]
}
```

FastAPI automatically validates that `from_date` and `to_date` are valid dates (if provided), returning a helpful 422 error otherwise. The `RealDictCursor` returns rows as dictionaries rather than tuples, making the code more readable.

---

#### Step 4: Create `static/index.html` — the frontend

A single self-contained HTML file. No external CSS framework. Chart.js loaded from a CDN. All JavaScript is inline.

**HTML structure:**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>NSE Stock Price Viewer</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    /* Minimal layout CSS */
    body { font-family: sans-serif; max-width: 960px; margin: 40px auto; padding: 0 16px; }
    #controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
    #symbol-input { width: 180px; }
    #status { color: #666; min-height: 20px; margin-bottom: 8px; }
    #status.error { color: #dc2626; }
    #summary { font-size: 14px; color: #444; margin-bottom: 8px; display: none; }
    canvas { display: none; }
  </style>
</head>
<body>
  <h2>NSE Stock Price Viewer</h2>

  <!-- Controls bar -->
  <div id="controls">
    <input id="symbol-input" list="symbol-list" placeholder="Symbol e.g. RELIANCE" autocomplete="off" />
    <datalist id="symbol-list"></datalist>  <!-- populated by JS on load -->

    <select id="series-select">
      <option value="EQ" selected>EQ – Normal equity</option>
      <option value="BE">BE – Trade-for-trade</option>
      <option value="SM">SM – Small & medium cap</option>
    </select>

    <label>From <input type="date" id="from-date" /></label>
    <label>To   <input type="date" id="to-date" /></label>

    <button id="load-btn">Load</button>
  </div>

  <div id="status">Loading symbol list…</div>

  <!-- Summary strip shown above chart after a successful load -->
  <div id="summary">
    <strong id="summary-symbol"></strong> &nbsp;|&nbsp;
    <span id="summary-count"></span> trading days &nbsp;|&nbsp;
    Latest close: ₹<span id="summary-close"></span>
  </div>

  <!-- Chart.js renders into this canvas -->
  <canvas id="price-chart" height="400"></canvas>

  <script>
    /* All JavaScript here — no separate .js file needed */
  </script>
</body>
</html>
```

**JavaScript — step-by-step logic:**

*1. Page load setup:*
```javascript
let chart = null;  // holds the Chart.js instance; destroyed and recreated on each load

document.addEventListener('DOMContentLoaded', async () => {
    // Set default date range: today and 1 year ago
    const today = new Date().toISOString().slice(0, 10);
    const oneYearAgo = new Date(Date.now() - 365 * 86400 * 1000).toISOString().slice(0, 10);
    document.getElementById('to-date').value = today;
    document.getElementById('from-date').value = oneYearAgo;

    // Fetch all symbols and populate the <datalist> for autocomplete
    try {
        const resp = await fetch('/api/symbols');
        const data = await resp.json();
        const list = document.getElementById('symbol-list');
        data.symbols.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            list.appendChild(opt);
        });
        setStatus('');  // clear "Loading symbol list..." message
    } catch (e) {
        setStatus('Could not load symbol list. Is the server running?', true);
    }
});
```

*2. Load button click handler:*
```javascript
document.getElementById('load-btn').addEventListener('click', loadChart);
document.getElementById('symbol-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') loadChart();
});

async function loadChart() {
    const symbol = document.getElementById('symbol-input').value.trim().toUpperCase();
    const series = document.getElementById('series-select').value;
    const fromDate = document.getElementById('from-date').value;
    const toDate   = document.getElementById('to-date').value;

    if (!symbol) {
        setStatus('Please enter a symbol.', true);
        return;
    }

    setStatus('Loading…');

    const params = new URLSearchParams({ symbol, series, from_date: fromDate, to_date: toDate });
    try {
        const resp = await fetch(`/api/history?${params}`);
        if (!resp.ok) {
            const err = await resp.json();
            setStatus(`Error ${resp.status}: ${err.detail}`, true);
            return;
        }
        const data = await resp.json();
        if (data.count === 0) {
            setStatus(`No data found for ${symbol} (${series}) between ${fromDate} and ${toDate}.`, true);
            document.getElementById('summary').style.display = 'none';
            document.getElementById('price-chart').style.display = 'none';
            return;
        }
        renderChart(data);
        setStatus('');
    } catch (e) {
        setStatus('Network error. Check that the server is running.', true);
    }
}
```

*3. `renderChart(data)`:*
```javascript
function renderChart(data) {
    const labels = data.data.map(r => r.date);
    const closes = data.data.map(r => r.close);

    // Destroy previous chart instance before creating a new one
    if (chart) chart.destroy();

    const canvas = document.getElementById('price-chart');
    canvas.style.display = 'block';

    chart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: `${data.symbol} (${data.series}) — Close`,
                data: closes,
                borderColor: '#2563eb',
                backgroundColor: 'rgba(37,99,235,0.08)',
                borderWidth: 1.5,
                tension: 0,          // straight lines, not bezier curves
                pointRadius: 0,      // hide dots for large date ranges
                fill: true,
            }]
        },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        // Show full OHLC + volume on hover
                        afterBody(items) {
                            const idx = items[0].dataIndex;
                            const row = data.data[idx];
                            return [
                                `Open:   ₹${row.open?.toFixed(2)}`,
                                `High:   ₹${row.high?.toFixed(2)}`,
                                `Low:    ₹${row.low?.toFixed(2)}`,
                                `Close:  ₹${row.close?.toFixed(2)}`,
                                `Volume: ${row.volume?.toLocaleString('en-IN')}`,
                            ];
                        }
                    }
                }
            },
            scales: {
                x: { ticks: { maxTicksLimit: 12 } },  // don't crowd the x-axis
                y: {
                    ticks: {
                        callback: v => `₹${v.toLocaleString('en-IN')}`  // rupee symbol on y-axis
                    }
                }
            }
        }
    });

    // Update summary bar
    const latest = data.data[data.data.length - 1];
    document.getElementById('summary-symbol').textContent = `${data.symbol} (${data.series})`;
    document.getElementById('summary-count').textContent = data.count;
    document.getElementById('summary-close').textContent = latest.close?.toFixed(2);
    document.getElementById('summary').style.display = 'block';
}
```

*4. Helper:*
```javascript
function setStatus(msg, isError = false) {
    const el = document.getElementById('status');
    el.textContent = msg;
    el.className = isError ? 'error' : '';
}
```

**Why a line chart, not candlestick?**
Chart.js 4 does not include candlestick support natively. Adding it requires a third-party plugin (chartjs-chart-financial). For a toy frontend, a close-price line chart clearly shows trends and is readable even with 5+ years of data. The tooltip already surfaces OHLC values on hover for anyone who wants the detail.

**Why `<datalist>` with all symbols loaded upfront?**
There are ~2000 symbols, each ~10 bytes — roughly 20KB of JSON, fetched once. The browser's native `<datalist>` filtering is instant and zero-code. The alternative (search-as-you-type with a backend query per keystroke) would require debounce logic, per-keystroke DB queries, and dynamic DOM manipulation — all for data that is entirely static once loaded. 20KB is a trivial one-time cost.

---

#### Step 5: Run and verify

```bash
# 1. Install new dependencies
uv sync

# 2. Make sure Postgres is running with data loaded
docker compose up -d
psql $DATABASE_URL -c "SELECT count(*) FROM bhav_copy;"

# 3. Start the FastAPI server (auto-reloads on file changes)
uv run uvicorn app:app --reload

# 4. Test the API endpoints directly
curl -s http://localhost:8000/api/symbols | python3 -c \
  "import sys, json; d=json.load(sys.stdin); print(len(d['symbols']), 'symbols')"
# Expected: ~2000 symbols

curl -s "http://localhost:8000/api/history?symbol=RELIANCE&series=EQ&from_date=2025-01-01" | python3 -c \
  "import sys, json; d=json.load(sys.stdin); print(d['count'], 'rows, first:', d['data'][0])"
# Expected: count > 0 with ohlc data

# 5. Open in browser
open http://localhost:8000/
```

---

### Verification Checklist

| What to check | Expected outcome |
|---|---|
| `uv run uvicorn app:app --reload` | Server starts, no errors |
| `GET /api/symbols` | JSON with ~2000 symbol strings |
| `GET /api/history?symbol=RELIANCE&series=EQ` | JSON with `count > 0` and OHLCV array |
| `GET /api/history?symbol=NOTREAL` | JSON with `count: 0`, status 200 |
| `GET /api/history` with `from_date` > `to_date` | HTTP 400 with error message |
| `GET /docs` | FastAPI auto-generated Swagger UI |
| Browser: page load | No JS console errors; "Loading symbol list…" briefly then clears |
| Browser: type "REL" in symbol input | Autocomplete suggests RELIANCE, RELINFRA, etc. |
| Browser: select RELIANCE, click Load | Line chart appears with ~240 data points for past year |
| Browser: hover over chart | Tooltip shows date + Open/High/Low/Close/Volume |
| Browser: type "NOTREAL", click Load | "No data found" error message shown |
| Browser: clear symbol field, click Load | "Please enter a symbol" validation message |

---

### Final File Tree After Implementation

```
/Users/kritarth/code/nse_bhav/
├── app.py                    ← NEW
├── static/
│   └── index.html            ← NEW
├── nse_bhav_copy.py          unchanged
├── load_to_db.py             unchanged
├── tests/
│   └── test_nse_bhav_copy.py unchanged
├── pyproject.toml            ← MODIFIED (3 new dependencies)
├── docker-compose.yml        unchanged
├── .env                      unchanged
├── plans.md                  ← this file
└── uv.lock                   auto-regenerated by uv sync
```
