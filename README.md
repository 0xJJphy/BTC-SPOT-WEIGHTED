# BTC-SPOT-WEIGHTED

A BTC multi-exchange data analytics pipeline that fetches 1D OHLCV data from 9 exchanges, calculates volume-weighted aggregated candles, and stores everything in PostgreSQL.

## Features

- **Multi-Exchange Support**: Binance, Bybit, OKX, Coinbase, Crypto.com, Hyperliquid, Upbit, Bitget, MEXC
- **PostgreSQL Storage**: Normalized schema with lookup tables for efficient storage
- **Premium Metrics**: Coinbase & Kimchi premium calculations
- **Volume-Weighted Aggregation**: VWAP across exchanges with outlier filtering
- **Daily Exchange Weights**: Track market share by volume
- **CSV Export**: Optional export to CSV files (disabled by default)
- **Automated Updates**: GitHub Actions for daily scheduled execution

## Database Schema

```
┌─────────────────┐      ┌─────────────────┐
│   exchanges     │      │    symbols      │
├─────────────────┤      ├─────────────────┤
│ id (PK)         │      │ id (PK)         │
│ name            │      │ name            │
│ created_at      │      │ base_currency   │
└────────┬────────┘      │ quote_currency  │
         │               └────────┬────────┘
         │                        │
         ▼                        ▼
┌─────────────────────────────────────────┐
│            ohlcv_daily                  │
├─────────────────────────────────────────┤
│ id (PK)                                 │
│ exchange_id (FK) ──► exchanges.id       │
│ symbol_id (FK) ──► symbols.id           │
│ timestamp                               │
│ open, high, low, close, volume          │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│       aggregated_usd_daily              │
├─────────────────────────────────────────┤
│ timestamp (PK)                          │
│ open, high, low, close, volume_usd      │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│          exchange_weights               │
├─────────────────────────────────────────┤
│ date                                    │
│ exchange_id (FK), symbol_id (FK)        │
│ notional_usd, weight                    │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│          premiums_daily                 │
├─────────────────────────────────────────┤
│ date (PK)                               │
│ coinbase_premium_pct                    │
│ kimchi_premium_pct                      │
│ usdkrw_rate                             │
└─────────────────────────────────────────┘

Views: v_ohlcv_daily, v_exchange_weights (join with names)
```

## Quick Start

### With Docker
```bash
# Build and run (first time)
docker-compose build
docker-compose up -d

# Or in one command
docker-compose up -d --build

# View logs
docker-compose logs -f pipeline

# Stop
docker-compose down
```

### With Supabase
```bash
# 1. Run schema.sql in Supabase SQL Editor
# 2. Set environment
cp .env.example .env
# Edit .env with your Supabase DATABASE_URL

# 3. Run
python btc_spot_pipeline.py
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | localhost | PostgreSQL connection string |
| `EXPORT_CSV` | `false` | Enable CSV file export |
| `DATA_DIR` | `exchange_data` | CSV output directory |

## Query Examples

```sql
-- Per-exchange data (using view)
SELECT * FROM v_ohlcv_daily 
WHERE exchange = 'Binance' AND symbol = 'BTC/USDT'
ORDER BY timestamp DESC LIMIT 10;

-- Using IDs directly (faster)
SELECT * FROM ohlcv_daily WHERE exchange_id = 1;

-- Daily weights
SELECT * FROM v_exchange_weights 
WHERE date = CURRENT_DATE - 1 
ORDER BY weight DESC;

-- Premiums
SELECT date, coinbase_premium_pct, kimchi_premium_pct 
FROM premiums_daily ORDER BY date DESC LIMIT 30;
```

## Files

| File | Description |
|------|-------------|
| `btc_spot_pipeline.py` | Main pipeline (~900 lines) |
| `schema.sql` | PostgreSQL schema (normalized) |
| `docker-compose.yml` | Docker services |
| `.env.example` | Environment template |

## License

MIT License