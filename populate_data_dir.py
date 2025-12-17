from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from clientside_data_endpoint import fetch_parquet_file
from configs import DIR_TO_FILE_PREFIX, DIR_TO_START_DATE, PARQUET_DIRS


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


def populate_data_dir() -> None:
    end_date = _utc_end_date()
    for dest_dir in PARQUET_DIRS:
        prefix = DIR_TO_FILE_PREFIX[dest_dir]
        start_date = _parse_yyyymmdd(DIR_TO_START_DATE[dest_dir])
        if start_date > end_date:
            print(f"Skipping {dest_dir} (start date after end date)")
            continue
        dest_dir.mkdir(parents=True, exist_ok=True)
        for day in _date_range(start_date, end_date):
            key = f"{prefix}{day.strftime('%Y%m%d')}"
            out_path = dest_dir / f"{key}.parquet"
            if out_path.exists():
                print(f"Skipping {key} (already exists)")
                continue
            print(f"Downloading {key}...")
            try:
                saved_path = fetch_parquet_file(key, dest_dir)
            except Exception as exc:
                print(f"Failed {key}: {exc}")
                continue
            print(f"Saved {key} -> {saved_path}")


if __name__ == "__main__":
    populate_data_dir()
