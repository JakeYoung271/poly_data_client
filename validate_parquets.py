#!/usr/bin/env python3
"""Validate all parquet files in the data directory."""

from pathlib import Path

try:
    import pyarrow.parquet as pq
except Exception as exc:
    raise SystemExit(f"pyarrow is required: {exc}")


def validate_parquet(path: Path) -> tuple[bool, str]:
    """Check if a parquet file is valid and return status with details."""
    try:
        pf = pq.ParquetFile(path)
        metadata = pf.metadata
        return True, f"{metadata.num_rows} rows, {metadata.num_columns} cols"
    except Exception as e:
        return False, str(e)


def main() -> None:
    data_dir = Path("data")
    parquet_files = list(data_dir.rglob("*.parquet"))

    if not parquet_files:
        print("No parquet files found in data directory.")
        return

    valid_count = 0
    invalid_count = 0
    invalid_files = []

    print(f"Checking {len(parquet_files)} parquet files...\n")

    for path in sorted(parquet_files):
        is_valid, details = validate_parquet(path)
        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1
            invalid_files.append((path, details))
            print(f"❌ INVALID: {path}")
            print(f"   Error: {details}\n")

    print("-" * 50)
    print(f"Total files: {len(parquet_files)}")
    print(f"Valid: {valid_count}")
    print(f"Invalid: {invalid_count}")

    if invalid_files:
        print("\nInvalid files:")
        for path, error in invalid_files:
            print(f"  - {path}")


if __name__ == "__main__":
    main()
