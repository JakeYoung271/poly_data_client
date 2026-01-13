from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from clientside_data_endpoint import fetch_parquet_file, fetch_wss_day
from configs import (
    DIR_TO_FILE_PREFIX,
    DIR_TO_START_DATE,
    PARQUET_DIRS,
    WSS_DIR,
    WSS_START_DATE,
)


def _utc_end_date() -> date:
    now = datetime.now(timezone.utc)
    cutoff_days = 1 if now.hour >= 3 else 2
    return now.date() - timedelta(days=cutoff_days)


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_yyyymmdd(raw: str) -> date:
    return datetime.strptime(raw, "%Y%m%d").date()


def populate_parquet_dirs() -> None:
    """Populate standard parquet data directories."""
    end_date = _utc_end_date()
    for dest_dir in PARQUET_DIRS:
        prefix = DIR_TO_FILE_PREFIX[dest_dir]
        start_date = _parse_yyyymmdd(DIR_TO_START_DATE[dest_dir])
        if start_date > end_date:
            print(f"Skipping {dest_dir} (start date after end date)")
            continue
        dest_dir.mkdir(parents=True, exist_ok=True)
        skip_start = None
        skip_end = None
        for day in _date_range(start_date, end_date):
            key = f"{prefix}{day.strftime('%Y%m%d')}"
            out_path = dest_dir / f"{key}.parquet"
            if out_path.exists():
                if skip_start is None:
                    skip_start = day
                skip_end = day
                continue
            if skip_start is not None and skip_end is not None:
                print(
                    f"Skipped {skip_start.strftime('%Y%m%d')} - {skip_end.strftime('%Y%m%d')}"
                )
                skip_start = None
                skip_end = None
            print(f"Downloading {key}...")
            try:
                saved_path = fetch_parquet_file(key, dest_dir)
            except Exception as exc:
                print(f"Failed {key}: {exc}")
                continue
            print(f"Saved {key} -> {saved_path}")
        if skip_start is not None and skip_end is not None:
            print(
                f"Skipped {skip_start.strftime('%Y%m%d')} - {skip_end.strftime('%Y%m%d')}"
            )


def populate_wss_dir() -> None:
    """Populate WSS data directory with files for each date."""
    end_date = _utc_end_date()
    start_date = _parse_yyyymmdd(WSS_START_DATE)
    if start_date > end_date:
        print(f"Skipping WSS data (start date after end date)")
        return
    WSS_DIR.mkdir(parents=True, exist_ok=True)
    for day in _date_range(start_date, end_date):
        date_str = day.strftime("%Y%m%d")
        print(f"\n=== WSS data for {date_str} ===")
        try:
            fetch_wss_day(date_str, WSS_DIR)
        except Exception as exc:
            print(f"Failed {date_str}: {exc}")


def populate_data_dir() -> None:
    """Populate all data directories (parquet + WSS)."""
    print("=== Populating parquet directories ===")
    populate_parquet_dirs()
    print("\n=== Populating WSS directory ===")
    populate_wss_dir()


if __name__ == "__main__":
    populate_data_dir()
