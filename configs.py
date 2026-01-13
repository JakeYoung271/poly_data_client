from pathlib import Path

# Repository roots and data directories
REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
TRADES_DIR = DATA_DIR / "historical_trades"
ALL_ORDERBOOKS_DIR = DATA_DIR / "historical_all_orderbooks"
HF_ORDERBOOKS_DIR = DATA_DIR / "historical_orderbooks"
SUBGRAPH_TRADES_DIR = DATA_DIR / "historical_subgraph_trades"
WSS_DIR = DATA_DIR / "wss"

PARQUET_DIRS = [TRADES_DIR, ALL_ORDERBOOKS_DIR, HF_ORDERBOOKS_DIR, SUBGRAPH_TRADES_DIR]

DIR_TO_FILE_PREFIX = {
    TRADES_DIR: "trades_",
    ALL_ORDERBOOKS_DIR: "all_orderbooks_",
    HF_ORDERBOOKS_DIR: "hf_orderbooks_",
    SUBGRAPH_TRADES_DIR: "compressed_trades_",
}

DIR_TO_START_DATE = {
    TRADES_DIR: "20251205",
    ALL_ORDERBOOKS_DIR: "20251129",
    HF_ORDERBOOKS_DIR: "20251129",
    SUBGRAPH_TRADES_DIR: "20251129",
}

# WSS data configuration: multi-part files per day
WSS_FILE_TYPES = {
    "wss_trades": "parquet",
    "wss_book_updates": "parquet",
    "wss_coverage": "parquet",
    "wss_tick_size_changes": "txt",
}

WSS_START_DATE = "20260110"

DATA_ENDPOINT = "https://data.youngjake.com"
