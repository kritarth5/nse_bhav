"""
Unit tests for nse_bhav_copy.py.

All HTTP calls are mocked — no real network traffic is made.
"""

import argparse
import io
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from nse_bhav_copy import (
    LARGE_RANGE_THRESHOLD,
    TodayDataUnavailableError,
    _FORMAT_SWITCH,
    _check_large_range,
    _fetch,
    _format_duration,
    _normalise,
    _parse_date,
    _url_for,
    _weekdays,
    run_download,
)


# ---------------------------------------------------------------------------
# Shared test data and helpers
# ---------------------------------------------------------------------------

# Minimal old-format (pre-2024-07-08) bhav CSV with three rows:
#   RELIANCE  EQ
#   TCS       EQ
#   INFY      BE  ← used by series-filter tests
OLD_FORMAT_CSV = (
    "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,"
    "TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN\n"
    "RELIANCE,EQ,2800,2850,2790,2830,2830,2810,1000000,2830000000,03-JAN-2024,50000,INE002A01018\n"
    "TCS,EQ,3500,3550,3480,3520,3520,3510,500000,1760000000,03-JAN-2024,30000,INE467B01029\n"
    "INFY,BE,1500,1530,1490,1520,1520,1510,200000,304000000,03-JAN-2024,15000,INE009A01021\n"
)

# Minimal new-format (UDiFF, post-2024-07-08) bhav CSV.
# Only the 13 columns that _normalise actually maps are included;
# _normalise works by column name so extra/missing columns don't matter.
NEW_FORMAT_CSV = (
    "TradDt,ISIN,TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,"
    "LastPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd\n"
    "2024-07-08,INE002A01018,RELIANCE,EQ,2800,2850,2790,2830,2830,2810,1000000,2830000000,50000\n"
    "2024-07-08,INE467B01029,TCS,EQ,3500,3550,3480,3520,3520,3510,500000,1760000000,30000\n"
    "2024-07-08,INE009A01021,INFY,BE,1500,1530,1490,1520,1520,1510,200000,304000000,15000\n"
)


def _zip_bytes(csv: str, filename: str = "bhav.csv") -> bytes:
    """Pack a CSV string into an in-memory ZIP and return the raw bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, csv)
    return buf.getvalue()


def _mock_session(status_code: int, content: bytes = b"") -> MagicMock:
    """Return a mock requests.Session whose .get() returns a fixed response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    session = MagicMock()
    session.get.return_value = resp
    return session


# ---------------------------------------------------------------------------
# 1. URL generation
# ---------------------------------------------------------------------------

class TestUrlFor:
    def test_legacy_format_before_switch(self):
        """Dates before the format switch should use the old archive URL."""
        url = _url_for(date(2024, 7, 5))  # last day of old format
        assert "historical/EQUITIES" in url
        assert "05JUL2024bhav.csv.zip" in url

    def test_udiff_format_on_switch_date(self):
        """The switch date itself (2024-07-08) should use the new UDiFF URL."""
        url = _url_for(_FORMAT_SWITCH)
        assert "BhavCopy_NSE_CM" in url
        assert "20240708" in url

    def test_udiff_format_after_switch(self):
        """Any date after the switch should use the new UDiFF URL."""
        url = _url_for(date(2025, 6, 15))
        assert "BhavCopy_NSE_CM" in url
        assert "20250615" in url

    def test_old_url_encodes_month_uppercase(self):
        """Old URL uses uppercase 3-letter month abbreviation."""
        url = _url_for(date(2023, 3, 1))
        assert "MAR" in url
        assert "mar" not in url


# ---------------------------------------------------------------------------
# 2. Weekday utilities
# ---------------------------------------------------------------------------

class TestWeekdays:
    def test_excludes_saturday(self):
        # 2026-02-21 is a Saturday
        assert _weekdays(date(2026, 2, 21), date(2026, 2, 21)) == []

    def test_excludes_sunday(self):
        # 2026-02-22 is a Sunday
        assert _weekdays(date(2026, 2, 22), date(2026, 2, 22)) == []

    def test_full_mon_to_fri_week(self):
        days = _weekdays(date(2026, 2, 16), date(2026, 2, 20))  # Mon–Fri
        assert len(days) == 5
        assert all(d.weekday() < 5 for d in days)

    def test_empty_when_start_after_end(self):
        assert _weekdays(date(2026, 2, 20), date(2026, 2, 16)) == []

    def test_range_spanning_a_weekend(self):
        # Mon 16 Feb to Mon 23 Feb = 6 weekdays (16,17,18,19,20 and 23)
        days = _weekdays(date(2026, 2, 16), date(2026, 2, 23))
        assert len(days) == 6

    def test_single_weekday_returns_one_item(self):
        # 2026-02-16 is a Monday
        days = _weekdays(date(2026, 2, 16), date(2026, 2, 16))
        assert days == [date(2026, 2, 16)]


# ---------------------------------------------------------------------------
# 3. Date parsing
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_format(self):
        assert _parse_date("2025-01-31") == date(2025, 1, 31)

    def test_dmy_hyphen_format(self):
        assert _parse_date("31-01-2025") == date(2025, 1, 31)

    def test_dmy_slash_format(self):
        assert _parse_date("31/01/2025") == date(2025, 1, 31)

    def test_invalid_string_raises(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_date("not-a-date")

    def test_wrong_field_order_raises(self):
        # YYYY/DD/MM is not one of the accepted formats
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_date("2025/31/01")


# ---------------------------------------------------------------------------
# 4. Column normalisation
# ---------------------------------------------------------------------------

class TestNormalise:
    def test_old_format_maps_symbol_and_close(self):
        df = pd.DataFrame([{
            "SYMBOL": "RELIANCE", "SERIES": "EQ",
            "OPEN": 2800, "HIGH": 2850, "LOW": 2790, "CLOSE": 2830,
            "LAST": 2830, "PREVCLOSE": 2810,
            "TOTTRDQTY": 1_000_000, "TOTTRDVAL": 2_830_000_000,
            "TIMESTAMP": "03-JAN-2024", "TOTALTRADES": 50_000,
            "ISIN": "INE002A01018",
        }])
        result = _normalise(df, date(2024, 1, 3))
        assert result["symbol"].iloc[0] == "RELIANCE"
        assert result["close"].iloc[0] == 2830

    def test_new_format_maps_ticker_and_close(self):
        df = pd.DataFrame([{
            "TckrSymb": "TCS", "SctySrs": "EQ",
            "OpnPric": 3500, "HghPric": 3550, "LwPric": 3480, "ClsPric": 3520,
            "LastPric": 3520, "PrvsClsgPric": 3510,
            "TtlTradgVol": 500_000, "TtlTrfVal": 1_760_000_000,
            "TradDt": "2024-07-08", "TtlNbOfTxsExctd": 30_000,
            "ISIN": "INE467B01029",
        }])
        result = _normalise(df, date(2024, 7, 8))
        assert result["symbol"].iloc[0] == "TCS"
        assert result["close"].iloc[0] == 3520

    def test_date_column_is_overridden_by_parameter(self):
        """The 'date' column must always reflect the date passed in, not raw CSV values."""
        df = pd.DataFrame([{
            "SYMBOL": "X", "SERIES": "EQ",
            "OPEN": 100, "HIGH": 110, "LOW": 90, "CLOSE": 105,
            "LAST": 105, "PREVCLOSE": 100,
            "TOTTRDQTY": 1000, "TOTTRDVAL": 105_000,
            "TIMESTAMP": "01-JAN-2000",   # deliberately wrong raw value
            "TOTALTRADES": 100, "ISIN": "IN0000000000",
        }])
        result = _normalise(df, date(2024, 1, 3))
        assert result["date"].iloc[0] == "2024-01-03"

    def test_output_has_exactly_the_expected_columns(self):
        df = pd.DataFrame([{
            "SYMBOL": "X", "SERIES": "EQ",
            "OPEN": 1, "HIGH": 1, "LOW": 1, "CLOSE": 1,
            "LAST": 1, "PREVCLOSE": 1,
            "TOTTRDQTY": 1, "TOTTRDVAL": 1,
            "TIMESTAMP": "01-JAN-2024", "TOTALTRADES": 1,
            "ISIN": "IN0000000000",
        }])
        result = _normalise(df, date(2024, 1, 1))
        expected = {
            "date", "symbol", "series",
            "open", "high", "low", "close", "last_price", "prev_close",
            "volume", "turnover", "total_trades", "isin",
        }
        assert set(result.columns) == expected


# ---------------------------------------------------------------------------
# 5. HTTP fetch (all mocked — no real network calls)
# ---------------------------------------------------------------------------

class TestFetch:
    def test_404_returns_none(self):
        """A 404 from NSE (holiday / weekend / future date) must return None."""
        result, status = _fetch(_mock_session(404), date(2025, 1, 15), verbose=False)
        assert result is None
        assert "404" in status

    def test_200_with_old_format_csv(self):
        """A 200 response with a valid old-format ZIP must return a normalised DataFrame."""
        d = date(2024, 1, 3)  # before format switch
        result, status = _fetch(
            _mock_session(200, _zip_bytes(OLD_FORMAT_CSV)), d, verbose=False
        )
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert {"symbol", "close", "date"}.issubset(result.columns)

    def test_200_with_new_format_csv(self):
        """A 200 response with a valid UDiFF ZIP must return a normalised DataFrame."""
        d = date(2024, 7, 8)  # on the format-switch date
        result, status = _fetch(
            _mock_session(200, _zip_bytes(NEW_FORMAT_CSV)), d, verbose=False
        )
        assert result is not None
        assert result["symbol"].tolist() == ["RELIANCE", "TCS", "INFY"]

    def test_corrupted_zip_returns_none(self):
        """Garbage bytes that are not a valid ZIP must return None, not raise."""
        result, status = _fetch(
            _mock_session(200, b"this is not a zip"), date(2025, 1, 15), verbose=False
        )
        assert result is None

    def test_timeout_returns_none(self):
        """A requests.Timeout must be caught and return None with an informative status."""
        session = MagicMock()
        session.get.side_effect = requests.Timeout
        result, status = _fetch(session, date(2025, 1, 15), verbose=False)
        assert result is None
        assert "timed out" in status

    def test_unexpected_network_error_returns_none(self):
        """Any other exception during the request must be caught and return None."""
        session = MagicMock()
        session.get.side_effect = ConnectionError("network down")
        result, status = _fetch(session, date(2025, 1, 15), verbose=False)
        assert result is None

    def test_500_server_error_returns_none(self):
        result, status = _fetch(_mock_session(500), date(2025, 1, 15), verbose=False)
        assert result is None
        assert "500" in status


# ---------------------------------------------------------------------------
# 6. Today's data unavailable
# ---------------------------------------------------------------------------

class TestTodayUnavailable:
    def test_raises_when_today_returns_404(self, tmp_path):
        """--today mode with a 404 must raise TodayDataUnavailableError."""
        with patch("nse_bhav_copy._make_session", return_value=_mock_session(404)):
            with pytest.raises(TodayDataUnavailableError) as exc_info:
                run_download(
                    dates=[date.today()],
                    output_dir=tmp_path,
                    merge=True,
                    series_filter=None,
                    quiet=True,
                    today_mode=True,
                )
        # Error message should hint at publish time
        assert "PM IST" in str(exc_info.value)

    def test_no_error_when_today_returns_200(self, tmp_path):
        """--today mode with a 200 response must complete without raising."""
        with patch("nse_bhav_copy._make_session",
                   return_value=_mock_session(200, _zip_bytes(NEW_FORMAT_CSV))):
            run_download(
                dates=[date.today()],
                output_dir=tmp_path,
                merge=True,
                series_filter=None,
                quiet=True,
                today_mode=True,
            )

    def test_non_today_404_is_silently_skipped(self, tmp_path):
        """Without today_mode, a 404 for any date is skipped, not an error."""
        with patch("nse_bhav_copy._make_session", return_value=_mock_session(404)):
            success = run_download(
                dates=[date(2025, 1, 15)],
                output_dir=tmp_path,
                merge=True,
                series_filter=None,
                quiet=True,
                today_mode=False,
            )
        assert success == 0

    def test_run_download_returns_success_count(self, tmp_path):
        """run_download must return the number of days successfully downloaded."""
        d = date(2024, 1, 3)
        with patch("nse_bhav_copy._make_session",
                   return_value=_mock_session(200, _zip_bytes(OLD_FORMAT_CSV))):
            count = run_download(
                dates=[d],
                output_dir=tmp_path,
                merge=True,
                series_filter=None,
                quiet=True,
                today_mode=False,
            )
        assert count == 1


# ---------------------------------------------------------------------------
# 7. Large range warning
# ---------------------------------------------------------------------------

class TestLargeRangeWarning:
    def test_format_duration_under_60_seconds(self):
        assert _format_duration(45) == "45s"

    def test_format_duration_exactly_60_seconds(self):
        assert _format_duration(60) == "1 min"

    def test_format_duration_minutes(self):
        assert _format_duration(90) == "1 min"
        assert _format_duration(3599) == "59 min"

    def test_format_duration_hours(self):
        result = _format_duration(7200)
        assert "hr" in result
        assert "2.0" in result

    def test_no_output_below_threshold(self, capsys):
        _check_large_range(LARGE_RANGE_THRESHOLD - 1)
        assert capsys.readouterr().out == ""

    def test_no_output_at_exact_threshold(self, capsys):
        _check_large_range(LARGE_RANGE_THRESHOLD)
        assert capsys.readouterr().out == ""

    def test_warning_printed_above_threshold(self, capsys):
        _check_large_range(LARGE_RANGE_THRESHOLD + 1)
        out = capsys.readouterr().out
        assert "Note" in out
        assert str(LARGE_RANGE_THRESHOLD + 1) in out

    def test_warning_includes_time_estimate(self, capsys):
        # 500 days × 0.25s = 125s → "2 min"
        _check_large_range(500)
        out = capsys.readouterr().out
        assert any(unit in out for unit in ("s", "min", "hr"))

    def test_warning_mentions_holidays_skipped(self, capsys):
        _check_large_range(LARGE_RANGE_THRESHOLD + 1)
        assert "holiday" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# 8. Series filter
# ---------------------------------------------------------------------------

class TestSeriesFilter:
    def test_filter_keeps_only_matching_rows(self, tmp_path):
        """With --series EQ only EQ rows should appear in the output CSV."""
        d = date(2024, 1, 3)
        with patch("nse_bhav_copy._make_session",
                   return_value=_mock_session(200, _zip_bytes(OLD_FORMAT_CSV))):
            run_download(
                dates=[d],
                output_dir=tmp_path,
                merge=True,
                series_filter="EQ",
                quiet=True,
            )
        out = pd.read_csv(tmp_path / f"bhav_{d.strftime('%Y%m%d')}.csv")
        # OLD_FORMAT_CSV has RELIANCE(EQ), TCS(EQ), INFY(BE) → expect 2 rows
        assert len(out) == 2
        assert (out["series"] == "EQ").all()

    def test_filter_excludes_other_series(self, tmp_path):
        """The BE row (INFY) must not appear when filtering for EQ."""
        d = date(2024, 1, 3)
        with patch("nse_bhav_copy._make_session",
                   return_value=_mock_session(200, _zip_bytes(OLD_FORMAT_CSV))):
            run_download(
                dates=[d],
                output_dir=tmp_path,
                merge=True,
                series_filter="EQ",
                quiet=True,
            )
        out = pd.read_csv(tmp_path / f"bhav_{d.strftime('%Y%m%d')}.csv")
        assert "INFY" not in out["symbol"].values

    def test_filter_is_case_insensitive(self, tmp_path):
        """Passing lowercase 'eq' must behave the same as 'EQ'."""
        d = date(2024, 1, 3)
        with patch("nse_bhav_copy._make_session",
                   return_value=_mock_session(200, _zip_bytes(OLD_FORMAT_CSV))):
            run_download(
                dates=[d],
                output_dir=tmp_path,
                merge=True,
                series_filter="eq",
                quiet=True,
            )
        out = pd.read_csv(tmp_path / f"bhav_{d.strftime('%Y%m%d')}.csv")
        assert len(out) == 2
