import base64
import sys
from pathlib import Path
from urllib import request, error
from configs import DATA_ENDPOINT

USERNAME = "..."
PASSWORD = "..."


def fetch_parquet_file(key: str, dest_dir: Path | None = None) -> Path:
    dest_dir = Path.cwd()
    url = f"{DATA_ENDPOINT.rstrip('/')}/parquet/{key}"
    auth_raw = f"{USERNAME}:{PASSWORD}".encode("utf-8")
    auth = base64.b64encode(auth_raw).decode("ascii")
    req = request.Request(url, headers={"Authorization": f"Basic {auth}"})
    out_path = dest_dir / f"{key}.parquet"
    try:
        with request.urlopen(req) as resp, out_path.open("wb") as f:
            f.write(resp.read())
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Request failed: {exc.code} {exc.reason} {body}") from exc
    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python clientside_data_enpoint.py <prefix_yyyymmdd> [dest_dir]")
    key_arg = sys.argv[1]
    dest = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    path = fetch_parquet_file(key_arg, dest)
    print(f"Saved to {path}")
