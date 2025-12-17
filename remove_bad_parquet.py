from pathlib import Path

try:
    import pyarrow.parquet as pq
except Exception as exc:
    raise SystemExit(f"pyarrow is required: {exc}")


def is_valid_parquet(path: Path) -> bool:
    try:
        pq.ParquetFile(path).metadata
    except Exception:
        return False
    return True


def main() -> None:
    for path in Path("data").rglob("*.parquet"):
        if not is_valid_parquet(path):
            print(f"Removing {path}")
            path.unlink()


if __name__ == "__main__":
    main()
