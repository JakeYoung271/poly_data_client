# Data Client

## Setup

1. Create a `.env` file in the repo root with:
   ```
   USERNAME=your_username
   PASSWORD=your_password
   ```
2. (Optional) Ensure the data output directories exist by running:
   ```
   python populate_data_dir.py
   ```

## Download a single parquet file

Run:
```
python clientside_data_endpoint.py <prefix_yyyymmdd> [dest_dir]
```

Example:
```
python clientside_data_endpoint.py trades_20251205 data/historical_trades
```

This download should take roughly 20 minutes to complete.
