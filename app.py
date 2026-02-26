"""
FastAPI web frontend for NSE Bhav Copy data.

Run with:
    uv run uvicorn app:app --reload
Then open: http://localhost:8000/
"""

import os
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()  # loads DATABASE_URL from .env

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy .env.example to .env and fill in credentials."
    )

app = FastAPI(title="NSE Bhav Copy Viewer")


def _get_conn():
    """Open a fresh psycopg2 connection."""
    return psycopg2.connect(DATABASE_URL)


@app.get("/api/symbols")
def get_symbols():
    """Return all distinct EQ symbols sorted alphabetically."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM bhav_copy WHERE series = 'EQ' ORDER BY symbol"
            )
            rows = cur.fetchall()
    return {"symbols": [r[0] for r in rows]}


@app.get("/api/history")
def get_history(
    symbol: str = Query(..., description="NSE ticker symbol, e.g. RELIANCE"),
    series: str = Query("EQ", description="Trading series, e.g. EQ, BE, SM"),
    from_date: date = Query(None, description="Start date YYYY-MM-DD (default: 1 year ago)"),
    to_date: date = Query(None, description="End date YYYY-MM-DD (default: today)"),
):
    """Return OHLCV price history for a given symbol and date range."""
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
        WHERE symbol = %s
          AND series = %s
          AND date >= %s
          AND date <= %s
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
