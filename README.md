# Data Client

## Setup

1. Create a `.env` file in the repo root with:
   ```
   USERNAME=your_username
   PASSWORD=your_password
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

This download should take roughly 30 seconds to complete.

## Populate the full data directory:

Run:
```
python populate_data_dir.py
```
This should take about 20 minutes