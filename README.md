# BTC-SPOT-WEIGHTED

A BTC multi-exchange data analytics tool designed to fetch 1D OHLCV data, calculate market premiums (Coinbase and Kimchi), and generate volume-weighted aggregated candles.

## Features

- **Multi-Exchange Support**: Fetches data from Binance, Bybit, OKX, Coinbase, Crypto.com, Hyperliquid, Upbit, Bitget, and MEXC.
- **Automated Data Management**: Incremental data fetching (updates existing CSVs with missing days).
- **Premium Metrics**:
    - **Coinbase Premium**: BTC/USD vs. aggregated USD-like pairs.
    - **Kimchi Premium**: Upbit KRW/USD vs. aggregated USD reference.
- **Weighted Aggregation**: Calculates volume-weighted average price (VWAP) across all exchanges (excluding outliers).
- **Interactive Visualizations**: Generates rich HTML charts for individual pairs and aggregated data.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/0xJJphy/BTC-SPOT-WEIGHTED.git
   cd BTC-SPOT-WEIGHTED
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. (Optional) Create a `.env` file if specific API keys or environment variables are needed for future extensions.

## Usage

Run the main analysis notebook:

1. Open JupyterLab or Jupyter Notebook.
2. Navigate to `scripts/BTC-SPOTDATA.ipynb`.
3. Run all cells to update data and generate charts.

The results will be saved in:
- `exchange_data/`: Raw CSV files.
- `exchange_charts/`: HTML interactive charts and analysis tables.

## License

MIT License - see [LICENSE](LICENSE) for details.