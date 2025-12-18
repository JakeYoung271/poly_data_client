import base64
import sys
from pathlib import Path
from urllib import request, error
from configs import DATA_ENDPOINT
from dotenv import load_dotenv
import os
import tempfile
import time

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - surface a clean error when used.
    pq = None

#load .env variables
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

def _validate_parquet(path: Path) -> None:
    if pq is None:
        raise RuntimeError("pyarrow is required to validate parquet files.")
    pq.ParquetFile(path).metadata


def _print_progress(downloaded: int, total: int | None, start: float, width: int = 30) -> None:
    mb_downloaded = downloaded / (1024 * 1024)
    elapsed = max(time.monotonic() - start, 1e-6)
    mb_per_sec = mb_downloaded / elapsed
    if total:
        mb_total = total / (1024 * 1024)
        filled = int(width * downloaded / total)
        bar = "#" * filled + "-" * (width - filled)
        pct = int(downloaded * 100 / total)
        msg = f"\r[{bar}] {pct:3d}% ({mb_downloaded:,.1f} MB / {mb_total:,.1f} MB) {mb_per_sec:,.1f} MB/s"
    else:
        msg = f"\rDownloaded {mb_downloaded:,.1f} MB ({mb_per_sec:,.1f} MB/s)"
    sys.stdout.write(msg)
    sys.stdout.flush()


def fetch_parquet_file(key: str, dest_dir: Path | None = None) -> Path:
    url = f"{DATA_ENDPOINT.rstrip('/')}/parquet/{key}"
    auth_raw = f"{USERNAME}:{PASSWORD}".encode("utf-8")
    auth = base64.b64encode(auth_raw).decode("ascii")
    req = request.Request(url, headers={"Authorization": f"Basic {auth}"})
    out_path = dest_dir / f"{key}.parquet"
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=dest_dir, delete=False, suffix=".parquet.part"
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            with request.urlopen(req) as resp:
                total = resp.length
                downloaded = 0
                start_time = time.monotonic()
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp_file.write(chunk)
                    downloaded += len(chunk)
                    _print_progress(downloaded, total, start_time)
                sys.stdout.write("\n")
        _validate_parquet(tmp_path)
        tmp_path.replace(out_path)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(f"Request failed: {exc.code} {exc.reason} {body}") from exc
    except Exception:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()
        raise
    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python clientside_data_enpoint.py <prefix_yyyymmdd> [dest_dir]")
    key_arg = sys.argv[1]
    dest = Path(sys.argv[2]) if len(sys.argv) > 2 else Path.cwd()
    path = fetch_parquet_file(key_arg, dest)
    print(f"Saved to {path}")
