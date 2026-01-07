#!/usr/bin/env python3
"""
BTC Spot Data Pipeline
Fetches OHLCV data from multiple exchanges, calculates weights and premiums,
and stores everything in PostgreSQL.
"""

import os
import io
import time
import requests
import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

# Silence pandas SQLAlchemy warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://btc_user:btc_password@localhost:5432/btc_spot")
USER_AGENT = {"User-Agent": "BTC-Spot-Pipeline/1.0"}
HYPERLIQUID_WEIGHT_PENALTY = 0.1  # Reduce Hyperliquid weight in aggregation
FULL_HIST_ON_FIRST_RUN = True
ONLY_LAST_2_DAYS_IF_MISSING = True

# CSV Export (disabled by default)
EXPORT_CSV = os.getenv("EXPORT_CSV", "false").lower() == "true"
DATA_DIR = os.getenv("DATA_DIR", "exchange_data")

# =============================================================================
# DATABASE LAYER
# =============================================================================

def get_db_connection():
    """Get PostgreSQL connection using psycopg2."""
    import psycopg2
    return psycopg2.connect(DATABASE_URL)

def test_db_connection() -> bool:
    """Test database connectivity and print status."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM exchanges;")
        exchange_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"[DB] ✓ Connected successfully!")
        print(f"[DB] PostgreSQL: {version.split(',')[0]}")
        print(f"[DB] Exchanges in DB: {exchange_count}")
        return True
    except Exception as e:
        print(f"[DB] ✗ Connection FAILED: {e}")
        print(f"[DB] DATABASE_URL: {DATABASE_URL[:40]}...")
        return False


def get_or_create_exchange_id(cur, exchange_name: str) -> int:
    """Get exchange ID from lookup table, creating if needed."""
    cur.execute("SELECT id FROM exchanges WHERE name = %s", (exchange_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO exchanges (name) VALUES (%s) RETURNING id", (exchange_name,))
    return cur.fetchone()[0]

def get_or_create_symbol_id(cur, symbol_name: str) -> int:
    """Get symbol ID from lookup table, creating if needed."""
    cur.execute("SELECT id FROM symbols WHERE name = %s", (symbol_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    parts = symbol_name.split('/')
    base = parts[0] if len(parts) > 0 else symbol_name
    quote = parts[1] if len(parts) > 1 else ''
    cur.execute("INSERT INTO symbols (name, base_currency, quote_currency) VALUES (%s, %s, %s) RETURNING id",
                (symbol_name, base, quote))
    return cur.fetchone()[0]

def upsert_ohlcv(df: pd.DataFrame, exchange: str, symbol: str):
    """Insert or update OHLCV data using normalized exchange_id/symbol_id."""
    if df is None or df.empty:
        return
    
    import psycopg2.extras
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get or create IDs
    exchange_id = get_or_create_exchange_id(cur, exchange)
    symbol_id = get_or_create_symbol_id(cur, symbol)
    conn.commit()
    
    records = []
    for _, row in df.iterrows():
        records.append((
            exchange_id, symbol_id, row['timestamp'],
            float(row['open']), float(row['high']), float(row['low']),
            float(row['close']), float(row['volume'])
        ))
    
    query = """
        INSERT INTO ohlcv_daily (exchange_id, symbol_id, timestamp, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (exchange_id, symbol_id, timestamp) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume
    """
    psycopg2.extras.execute_values(cur, query, records)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Upserted {len(records)} rows for {exchange} {symbol}")

def upsert_aggregated(df: pd.DataFrame):
    """Insert or update aggregated USD candles."""
    if df is None or df.empty:
        return
    
    import psycopg2.extras
    conn = get_db_connection()
    cur = conn.cursor()
    
    records = [(row['timestamp'], float(row['open']), float(row['high']),
                float(row['low']), float(row['close']), float(row['volume_usd']))
               for _, row in df.iterrows()]
    
    query = """
        INSERT INTO aggregated_usd_daily (timestamp, open, high, low, close, volume_usd)
        VALUES %s
        ON CONFLICT (timestamp) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume_usd = EXCLUDED.volume_usd
    """
    psycopg2.extras.execute_values(cur, query, records)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Upserted {len(records)} aggregated rows")

def upsert_weights(df: pd.DataFrame):
    """Insert or update exchange weights using normalized IDs (optimized)."""
    if df is None or df.empty:
        return
    
    import psycopg2.extras
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Cache all exchanges and symbols upfront (much faster than per-row queries)
    cur.execute("SELECT name, id FROM exchanges")
    exchange_cache = {name: id for name, id in cur.fetchall()}
    
    cur.execute("SELECT name, id FROM symbols")
    symbol_cache = {name: id for name, id in cur.fetchall()}
    
    records = []
    for _, row in df.iterrows():
        # Parse "Exchange Symbol" format (e.g., "Binance BTC/USDT")
        name_parts = row['name'].split(' ', 1)
        exchange_name = name_parts[0]
        symbol_name = name_parts[1] if len(name_parts) > 1 else "BTC/USD"
        
        # Use cache, create if missing
        if exchange_name not in exchange_cache:
            exchange_id = get_or_create_exchange_id(cur, exchange_name)
            exchange_cache[exchange_name] = exchange_id
        else:
            exchange_id = exchange_cache[exchange_name]
            
        if symbol_name not in symbol_cache:
            symbol_id = get_or_create_symbol_id(cur, symbol_name)
            symbol_cache[symbol_name] = symbol_id
        else:
            symbol_id = symbol_cache[symbol_name]
        
        date_val = row['date'].date() if hasattr(row['date'], 'date') else row['date']
        records.append((date_val, exchange_id, symbol_id, float(row['notional_usd']), float(row['weight'])))
    
    conn.commit()
    
    query = """
        INSERT INTO exchange_weights (date, exchange_id, symbol_id, notional_usd, weight)
        VALUES %s
        ON CONFLICT (date, exchange_id, symbol_id) DO UPDATE SET
            notional_usd = EXCLUDED.notional_usd, weight = EXCLUDED.weight
    """
    psycopg2.extras.execute_values(cur, query, records)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[DB] Upserted {len(records)} weight records")


def upsert_premiums(cb_prem: pd.DataFrame, kimchi_prem: pd.DataFrame):
    """Insert or update premium data."""
    import psycopg2.extras
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Merge premiums by date
    records = []
    dates = set()
    
    if cb_prem is not None and not cb_prem.empty:
        for _, row in cb_prem.iterrows():
            d = row['date'].date() if hasattr(row['date'], 'date') else row['date']
            dates.add(d)
    
    if kimchi_prem is not None and not kimchi_prem.empty:
        for _, row in kimchi_prem.iterrows():
            d = row['date'].date() if hasattr(row['date'], 'date') else row['date']
            dates.add(d)
    
    for d in sorted(dates):
        cb_val = None
        kimchi_val = None
        usdkrw = None
        
        if cb_prem is not None and not cb_prem.empty:
            match = cb_prem[cb_prem['date'].dt.date == d] if hasattr(cb_prem['date'].iloc[0], 'date') else cb_prem[cb_prem['date'] == d]
            if not match.empty:
                cb_val = float(match.iloc[0]['coinbase_premium_pct'])
        
        if kimchi_prem is not None and not kimchi_prem.empty:
            match = kimchi_prem[kimchi_prem['date'].dt.date == d] if hasattr(kimchi_prem['date'].iloc[0], 'date') else kimchi_prem[kimchi_prem['date'] == d]
            if not match.empty:
                kimchi_val = float(match.iloc[0]['kimchi_pct'])
                usdkrw = float(match.iloc[0]['USDKRW'])
        
        records.append((d, cb_val, kimchi_val, usdkrw))
    
    if records:
        query = """
            INSERT INTO premiums_daily (date, coinbase_premium_pct, kimchi_premium_pct, usdkrw_rate)
            VALUES %s
            ON CONFLICT (date) DO UPDATE SET
                coinbase_premium_pct = COALESCE(EXCLUDED.coinbase_premium_pct, premiums_daily.coinbase_premium_pct),
                kimchi_premium_pct = COALESCE(EXCLUDED.kimchi_premium_pct, premiums_daily.kimchi_premium_pct),
                usdkrw_rate = COALESCE(EXCLUDED.usdkrw_rate, premiums_daily.usdkrw_rate)
        """
        psycopg2.extras.execute_values(cur, query, records)
        conn.commit()
        print(f"[DB] Upserted {len(records)} premium records")
    
    cur.close()
    conn.close()

def get_last_timestamp(exchange: str, symbol: str) -> Optional[datetime]:
    """Get the last timestamp for an exchange/symbol pair from DB (normalized schema)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT MAX(o.timestamp) 
            FROM ohlcv_daily o
            JOIN exchanges e ON o.exchange_id = e.id
            JOIN symbols s ON o.symbol_id = s.id
            WHERE e.name = %s AND s.name = %s
        """, (exchange, symbol))
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result
    except Exception:
        return None

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def to_ms(dt: datetime) -> int:
    """Convert datetime to milliseconds timestamp."""
    return int(dt.timestamp() * 1000)

def ms_to_utc(ms: int) -> datetime:
    """Convert milliseconds timestamp to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def today_utc_date():
    """Get today's date in UTC."""
    return datetime.now(timezone.utc).date()

def two_day_start_ms_utc() -> int:
    """Get timestamp for 2 days ago at midnight UTC."""
    dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    return to_ms(dt)

def normalize_ohlcv_rows(rows: List[List], symbol: str, exchange: str) -> pd.DataFrame:
    """Normalize raw OHLCV rows into a standardized DataFrame."""
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["symbol"] = symbol
    df["exchange"] = exchange
    df = df.drop(columns=["ts_ms"]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "exchange"]]

def ensure_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame has a 'date' column derived from timestamp."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "date" not in out.columns:
        out["date"] = out["timestamp"].dt.floor("D")
    return out

# =============================================================================
# EXCHANGE FETCHERS
# =============================================================================

class BinanceFetcher:
    """Fetch OHLCV from Binance spot API."""
    BASE = "https://api.binance.com/api/v3/klines"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "1d", start_ms: Optional[int] = None) -> pd.DataFrame:
        sym = symbol.replace("/", "")
        start = start_ms or to_ms(datetime(2019, 9, 1, tzinfo=timezone.utc))
        all_rows: List[List] = []
        while True:
            params = {"symbol": sym, "interval": interval, "limit": 1000, "startTime": start}
            r = requests.get(BinanceFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code != 200:
                print(f"[Binance] HTTP {r.status_code}"); break
            batch = r.json()
            if not batch: break
            for k in batch:
                all_rows.append([k[0], k[1], k[2], k[3], k[4], k[5]])
            if len(batch) < 1000: break
            start = batch[-1][0] + 1
            time.sleep(0.12)
        return normalize_ohlcv_rows(all_rows, symbol, "Binance")


class BybitFetcher:
    """Fetch OHLCV from Bybit spot API."""
    BASE = "https://api.bybit.com/v5/market/kline"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "D", start_ms: Optional[int] = None) -> pd.DataFrame:
        all_rows: List[List] = []
        end = to_ms(datetime.now(timezone.utc))
        while True:
            params = {"category": "spot", "symbol": symbol.replace("/", ""), "interval": interval, "limit": 1000, "end": end}
            if start_ms: params["start"] = start_ms
            r = requests.get(BybitFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code != 200: break
            js = r.json()
            if js.get("retCode", -1) != 0: break
            lst = js.get("result", {}).get("list", [])
            if not lst: break
            for k in lst:
                all_rows.append([int(k[0]), k[1], k[2], k[3], k[4], k[5]])
            if len(lst) < 1000: break
            end = int(lst[-1][0]) - 1
            time.sleep(0.12)
        return normalize_ohlcv_rows(all_rows, symbol, "Bybit")


class OKXFetcher:
    """Fetch OHLCV from OKX API with pagination."""
    BASE = "https://www.okx.com/api/v5/market/history-candles"
    BASE_RECENT = "https://www.okx.com/api/v5/market/candles"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "1D", start_ms: Optional[int] = None) -> pd.DataFrame:
        okx_symbol = symbol.replace("/", "-")
        all_rows, seen_ts = [], set()
        
        # Recent data
        try:
            r = requests.get(OKXFetcher.BASE_RECENT, params={"instId": okx_symbol, "bar": interval, "limit": 100}, headers=USER_AGENT, timeout=30)
            if r.status_code == 200:
                js = r.json()
                if js.get("code") == "0":
                    for k in js.get("data", []):
                        t = int(k[0])
                        if t not in seen_ts:
                            seen_ts.add(t)
                            all_rows.append([t, k[1], k[2], k[3], k[4], k[5]])
        except Exception as e:
            print(f"[OKX] Recent error: {e}")
        
        # Historical data
        after_ts = None
        for _ in range(50):
            params = {"instId": okx_symbol, "bar": interval, "limit": 100}
            if after_ts: params["after"] = after_ts
            try:
                r = requests.get(OKXFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
                if r.status_code != 200: break
                js = r.json()
                if js.get("code") != "0": break
                data = js.get("data", [])
                if not data: break
                oldest = None
                for k in data:
                    t = int(k[0])
                    if t not in seen_ts:
                        seen_ts.add(t)
                        all_rows.append([t, k[1], k[2], k[3], k[4], k[5]])
                        if oldest is None or t < oldest: oldest = t
                if oldest is None: break
                if start_ms and oldest <= start_ms: break
                after_ts = oldest
                time.sleep(0.15)
            except Exception:
                break
        
        if start_ms:
            all_rows = [r for r in all_rows if r[0] >= start_ms]
        all_rows.sort(key=lambda x: x[0])
        return normalize_ohlcv_rows(all_rows, symbol, "OKX")


class CoinbaseFetcher:
    """Fetch OHLCV from Coinbase Exchange API."""
    BASE = "https://api.exchange.coinbase.com/products/{}/candles"

    @staticmethod
    def fetch(symbol: str = "BTC/USD", granularity_sec: int = 86400, start_ms: Optional[int] = None) -> pd.DataFrame:
        product = "BTC-USD" if symbol in ("BTC/USD", "BTC/USDT", "BTC/USDC") else symbol.replace("/", "-")
        end_dt = datetime.now(timezone.utc)
        since_dt = datetime(2019, 9, 1, tzinfo=timezone.utc) if not start_ms else datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)
        step = granularity_sec * 300
        cur_end = end_dt
        all_rows: List[List] = []
        
        while cur_end > since_dt:
            cur_start = max(since_dt, cur_end - timedelta(seconds=step - granularity_sec))
            params = {"granularity": granularity_sec,
                      "start": cur_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      "end": cur_end.strftime('%Y-%m-%dT%H:%M:%SZ')}
            r = requests.get(CoinbaseFetcher.BASE.format(product), params=params, headers=USER_AGENT, timeout=30)
            if r.status_code == 404: break
            if r.status_code != 200: break
            batch = r.json()
            if not batch: break
            for k in batch:
                all_rows.append([k[0]*1000, k[3], k[2], k[1], k[4], k[5]])
            oldest = min(b[0] for b in batch)
            cur_end = datetime.fromtimestamp(oldest - granularity_sec, tz=timezone.utc)
            time.sleep(0.12)
        
        all_rows.sort(key=lambda r: r[0])
        return normalize_ohlcv_rows(all_rows, symbol, "Coinbase")


class CryptoComFetcher:
    """Fetch OHLCV from Crypto.com Exchange API."""
    BASE = "https://api.crypto.com/exchange/v1/public/get-candlestick"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "1D", start_ms: Optional[int] = None) -> pd.DataFrame:
        inst = symbol.replace("/", "_")
        all_rows: List[List] = []
        end_ts = to_ms(datetime.now(timezone.utc))
        since = start_ms or to_ms(datetime(2019, 9, 1, tzinfo=timezone.utc))
        
        while end_ts > since:
            params = {"instrument_name": inst, "timeframe": interval, "count": 300, "end_ts": end_ts}
            r = requests.get(CryptoComFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code != 200: break
            js = r.json()
            if js.get("code") != 0: break
            data = js.get("result", {}).get("data", [])
            if not data: break
            for k in data:
                all_rows.append([k['t'], k['o'], k['h'], k['l'], k['c'], k.get('v', 0)])
            oldest = min(k['t'] for k in data)
            end_ts = oldest - 1
            time.sleep(0.1)
        return normalize_ohlcv_rows(all_rows, symbol, "Crypto.com")


class HyperliquidFetcher:
    """Fetch OHLCV from Hyperliquid API."""
    ENDPOINTS = ["https://api.hyperliquid.xyz/info", "https://api-ui.hyperliquid.xyz/info"]

    @staticmethod
    def fetch(symbol: str = "BTC/USDC", interval: str = "1d", start_ms: Optional[int] = None) -> pd.DataFrame:
        coin = symbol.split('/')[0].upper()
        since = start_ms or to_ms(datetime(2019, 9, 1, tzinfo=timezone.utc))
        end_ms = to_ms(datetime.now(timezone.utc))
        payload = {"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": since, "endTime": end_ms}}
        
        js = None
        for url in HyperliquidFetcher.ENDPOINTS:
            try:
                r = requests.post(url, json=payload, headers=USER_AGENT, timeout=30)
                if r.status_code == 200:
                    js = r.json()
                    break
            except Exception:
                pass
        
        if not js or not isinstance(js, list):
            return pd.DataFrame()
        
        rows = []
        for k in js:
            rows.append([int(k.get("t", 0)), float(k.get("o", 0)), float(k.get("h", 0)),
                         float(k.get("l", 0)), float(k.get("c", 0)), float(k.get("v", 0))])
        return normalize_ohlcv_rows(rows, symbol, "Hyperliquid")


class UpbitFetcher:
    """Fetch OHLCV from Upbit API."""
    BASE = "https://api.upbit.com/v1"

    @staticmethod
    def fetch_days(symbol: str = "BTC/KRW", start_ms: Optional[int] = None) -> pd.DataFrame:
        market = "KRW-BTC" if symbol.upper().endswith("KRW") else "USDT-BTC"
        url = UpbitFetcher.BASE + "/candles/days"
        to_dt = datetime.now(timezone.utc)
        threshold_dt = datetime(2017, 1, 1, tzinfo=timezone.utc)
        all_rows: List[List] = []
        
        for _ in range(30):
            params = {"market": market, "count": 200, "to": to_dt.strftime("%Y-%m-%dT%H:%M:%S")}
            r = requests.get(url, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code == 429:
                time.sleep(1); continue
            if r.status_code != 200: break
            batch = r.json()
            if not batch or isinstance(batch, dict): break
            
            oldest_dt = None
            for k in batch:
                iso = k.get("candle_date_time_utc")
                if not iso: continue
                dt = datetime.fromisoformat(iso.replace('Z', '+00:00') if iso.endswith('Z') else iso)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                if oldest_dt is None or dt < oldest_dt: oldest_dt = dt
                ts_ms = int(dt.timestamp() * 1000)
                all_rows.append([ts_ms, float(k.get("opening_price", 0)), float(k.get("high_price", 0)),
                                float(k.get("low_price", 0)), float(k.get("trade_price", 0)),
                                float(k.get("candle_acc_trade_volume", 0))])
            
            if not oldest_dt or oldest_dt <= threshold_dt: break
            if start_ms and oldest_dt <= datetime.fromtimestamp(start_ms/1000, tz=timezone.utc): break
            if len(batch) < 200: break
            to_dt = oldest_dt - timedelta(seconds=1)
            time.sleep(0.15)
        
        return normalize_ohlcv_rows(all_rows, symbol, "Upbit")


class BitgetFetcher:
    """Fetch OHLCV from Bitget API."""
    BASE = "https://api.bitget.com/api/v2/spot/market/history-candles"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "1D", start_ms: Optional[int] = None) -> pd.DataFrame:
        sym = symbol.replace("/", "")
        gran = "1Dutc" if interval.lower() in ("1d", "1day") else interval
        end = to_ms(datetime.now(timezone.utc))
        all_rows, prev_oldest = [], None
        
        while True:
            params = {"symbol": sym, "granularity": gran, "endTime": end, "limit": 200}
            r = requests.get(BitgetFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code != 200: break
            js = r.json()
            if js.get("code") != "00000": break
            data = js.get("data", [])
            if not data: break
            for k in data:
                all_rows.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])])
            oldest = min(int(k[0]) for k in data)
            if start_ms and oldest <= start_ms: break
            if prev_oldest and oldest >= prev_oldest: break
            prev_oldest = oldest
            end = oldest - 1
            time.sleep(0.12)
        
        df = normalize_ohlcv_rows(all_rows, symbol, "Bitget")
        if start_ms:
            df = df[df["timestamp"] >= pd.to_datetime(start_ms, unit="ms", utc=True)]
        return df


class MEXCFetcher:
    """Fetch OHLCV from MEXC API."""
    BASE = "https://api.mexc.com/api/v3/klines"

    @staticmethod
    def fetch(symbol: str = "BTC/USDT", interval: str = "1d", start_ms: Optional[int] = None) -> pd.DataFrame:
        sym = symbol.replace("/", "")
        end = to_ms(datetime.now(timezone.utc))
        all_rows, prev_oldest = [], None
        
        while True:
            params = {"symbol": sym, "interval": interval, "limit": 1000, "endTime": end}
            r = requests.get(MEXCFetcher.BASE, params=params, headers=USER_AGENT, timeout=30)
            if r.status_code != 200: break
            batch = r.json()
            if not batch: break
            for k in batch:
                all_rows.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])])
            oldest = min(int(k[0]) for k in batch)
            if start_ms and oldest <= start_ms: break
            if prev_oldest and oldest >= prev_oldest: break
            prev_oldest = oldest
            end = oldest - 1
            time.sleep(0.12)
        return normalize_ohlcv_rows(all_rows, symbol, "MEXC")


# =============================================================================
# FX & PREMIUM CALCULATIONS
# =============================================================================

def fetch_usdkrw_timeseries(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch USD/KRW exchange rate from public sources."""
    for base_url in ("https://api.frankfurter.app", "https://api.frankfurter.dev"):
        try:
            url = f"{base_url}/{start_date}..{end_date}"
            r = requests.get(url, params={"from": "USD", "to": "KRW"}, headers=USER_AGENT, timeout=30)
            if r.status_code == 200:
                rates = r.json().get("rates", {})
                if rates:
                    rows = [{"date": pd.to_datetime(d, utc=True), "USDKRW": float(rec["KRW"])} for d, rec in sorted(rates.items())]
                    out = pd.DataFrame(rows).set_index("date").sort_index()
                    if out["USDKRW"].median() < 1:
                        out["USDKRW"] = 1.0 / out["USDKRW"]
                    print(f"[FX] USDKRW loaded: {len(out)} rows")
                    return out
        except Exception:
            pass
    print("[FX] Failed to fetch USDKRW")
    return pd.DataFrame()


def compute_coinbase_premium(cb_df: pd.DataFrame, usd_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Calculate Coinbase premium vs weighted USD reference."""
    if cb_df is None or cb_df.empty or not usd_dfs:
        return pd.DataFrame()
    
    cb_dates = ensure_daily(cb_df)[["date", "close", "volume"]].rename(columns={"close": "cb_close", "volume": "cb_vol"})
    
    ref_parts = []
    for d in usd_dfs:
        if d is None or d.empty: continue
        dd = ensure_daily(d)[["date", "close", "volume", "exchange"]].copy()
        dd["w_usd"] = dd["close"] * dd["volume"]
        dd.loc[dd["exchange"].str.contains("Hyperliquid", case=False, na=False), "w_usd"] *= HYPERLIQUID_WEIGHT_PENALTY
        ref_parts.append(dd[["date", "close", "w_usd"]])
    
    if not ref_parts:
        return pd.DataFrame()
    
    ref = pd.concat(ref_parts, ignore_index=True)
    ref_agg = ref.groupby("date").apply(lambda g: pd.Series({
        "ref_close": (g["close"] * g["w_usd"]).sum() / max(g["w_usd"].sum(), 1e-12)
    }), include_groups=False).reset_index()
    
    m = cb_dates.merge(ref_agg, on="date", how="inner")
    m["coinbase_premium_pct"] = (m["cb_close"] - m["ref_close"]) / m["ref_close"] * 100
    return m[["date", "cb_close", "ref_close", "coinbase_premium_pct"]]


def compute_kimchi_premium(upbit_df: pd.DataFrame, ref_usd_df: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Kimchi premium (Upbit vs USD reference)."""
    if upbit_df is None or upbit_df.empty or ref_usd_df is None or ref_usd_df.empty or fx_df is None or fx_df.empty:
        return pd.DataFrame()
    
    U = ensure_daily(upbit_df)[["date", "close"]].rename(columns={"close": "upbit_close_krw"})
    R = ensure_daily(ref_usd_df)[["date", "close"]].rename(columns={"close": "ref_close_usd"})
    F = fx_df.reset_index().rename(columns={"date": "date"})
    
    m = U.merge(R, on="date", how="inner").merge(F, on="date", how="left").sort_values("date")
    m["USDKRW"] = m["USDKRW"].ffill().bfill()
    m["upbit_close_usd"] = m["upbit_close_krw"] / m["USDKRW"]
    m["kimchi_pct"] = (m["upbit_close_usd"] - m["ref_close_usd"]) / m["ref_close_usd"] * 100
    return m[["date", "upbit_close_krw", "USDKRW", "upbit_close_usd", "ref_close_usd", "kimchi_pct"]]


# =============================================================================
# AGGREGATION & WEIGHTS
# =============================================================================

def aggregate_usd_candles(dfs: List[pd.DataFrame], outlier_factor: float = 0.35) -> pd.DataFrame:
    """Robust VWAP aggregation across USD exchanges."""
    parts = []
    for d in dfs:
        if d is None or d.empty: continue
        dd = ensure_daily(d)
        if "exchange" not in dd.columns:
            dd["exchange"] = "Unknown"
        parts.append(dd[["date", "open", "high", "low", "close", "volume", "exchange"]])
    
    if not parts:
        return pd.DataFrame()
    
    big = pd.concat(parts, ignore_index=True)
    big = big.dropna(subset=["open", "high", "low", "close", "volume"])
    big = big[(big["close"] > 0) & (big["volume"] > 0)]
    
    # Outlier filtering
    med = big.groupby("date")["close"].median().rename("med_close")
    big = big.merge(med, on="date", how="left")
    lo, hi = (1.0 - outlier_factor), (1.0 + outlier_factor)
    mask = (big["close"] >= lo * big["med_close"]) & (big["close"] <= hi * big["med_close"])
    big = big[mask].drop(columns=["med_close"])
    
    # Volume-weighted aggregation
    big["w_usd"] = big["close"] * big["volume"]
    big.loc[big["exchange"].str.contains("Hyperliquid", case=False, na=False), "w_usd"] *= HYPERLIQUID_WEIGHT_PENALTY
    
    grp = big.groupby("date")
    out = pd.DataFrame({
        "open": grp.apply(lambda g: np.average(g["open"], weights=g["w_usd"]), include_groups=False),
        "close": grp.apply(lambda g: np.average(g["close"], weights=g["w_usd"]), include_groups=False),
        "high": grp["high"].max(),
        "low": grp["low"].min(),
        "volume_usd": grp["w_usd"].sum()
    }).reset_index()
    
    out["timestamp"] = pd.to_datetime(out["date"], utc=True)
    print(f"[AGG] Aggregated {len(out)} days")
    return out[["timestamp", "open", "high", "low", "close", "volume_usd"]]


def compute_daily_weights(named_dfs: List[Tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """Compute daily volume weights per exchange pair."""
    frames = []
    for name, df in named_dfs:
        if df is None or df.empty: continue
        tmp = ensure_daily(df)[["date", "close", "volume"]].copy()
        tmp["name"] = name
        tmp["notional_usd"] = tmp["close"] * tmp["volume"]
        if "hyperliquid" in name.lower():
            tmp["notional_usd"] *= HYPERLIQUID_WEIGHT_PENALTY
        frames.append(tmp[["date", "name", "notional_usd"]])
    
    if not frames:
        return pd.DataFrame()
    
    allv = pd.concat(frames, ignore_index=True)
    allv = allv.groupby(["date", "name"], as_index=False)["notional_usd"].sum()
    sums = allv.groupby("date")["notional_usd"].sum().rename("tot_notional_usd")
    m = allv.merge(sums, on="date", how="left")
    m["weight"] = np.where(m["tot_notional_usd"] > 0, m["notional_usd"] / m["tot_notional_usd"], 0.0)
    return m[["date", "name", "notional_usd", "tot_notional_usd", "weight"]].sort_values(["date", "weight"], ascending=[True, False])


# =============================================================================
# CSV EXPORT (optional, disabled by default)
# =============================================================================

def export_to_csv(data: Dict[str, pd.DataFrame], aggregated: pd.DataFrame, 
                  weights: pd.DataFrame, cb_prem: pd.DataFrame, kimchi_prem: pd.DataFrame):
    """Export all data to CSV files if EXPORT_CSV is enabled."""
    if not EXPORT_CSV:
        return
    
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"\n[CSV] Exporting to {DATA_DIR}/...")
    
    # Export individual exchange data
    for key, df in data.items():
        if df is not None and not df.empty:
            filename = key.replace(":", "_").replace("/", "-") + ".csv"
            filepath = os.path.join(DATA_DIR, filename)
            df.to_csv(filepath, index=False)
            print(f"[CSV] {filename}: {len(df)} rows")
    
    # Export aggregated candles
    if aggregated is not None and not aggregated.empty:
        aggregated.to_csv(os.path.join(DATA_DIR, "aggregated_usd.csv"), index=False)
        print(f"[CSV] aggregated_usd.csv: {len(aggregated)} rows")
    
    # Export weights
    if weights is not None and not weights.empty:
        weights.to_csv(os.path.join(DATA_DIR, "exchange_weights.csv"), index=False)
        print(f"[CSV] exchange_weights.csv: {len(weights)} rows")
    
    # Export premiums
    if cb_prem is not None and not cb_prem.empty:
        cb_prem.to_csv(os.path.join(DATA_DIR, "coinbase_premium.csv"), index=False)
        print(f"[CSV] coinbase_premium.csv: {len(cb_prem)} rows")
    
    if kimchi_prem is not None and not kimchi_prem.empty:
        kimchi_prem.to_csv(os.path.join(DATA_DIR, "kimchi_premium.csv"), index=False)
        print(f"[CSV] kimchi_premium.csv: {len(kimchi_prem)} rows")
    
    print(f"[CSV] Export complete!")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main():
    """Main pipeline execution."""
    print("=" * 60)
    print("BTC Spot Data Pipeline - Starting")
    print("=" * 60)
    
    # Test database connection first
    print("\n=== Database Connection ===")
    if not test_db_connection():
        print("Exiting due to database connection failure.")
        return
    
    # Default start for first run (2019-09-01)
    default_start_ms = to_ms(datetime(2019, 9, 1, tzinfo=timezone.utc))
    
    # Define fetch tasks: (exchange, symbol, fetcher_function, interval_kwarg)
    tasks = [
        ("Binance", "BTC/USDT", BinanceFetcher.fetch, {"interval": "1d"}),
        ("Binance", "BTC/USDC", BinanceFetcher.fetch, {"interval": "1d"}),
        ("Bybit", "BTC/USDT", BybitFetcher.fetch, {"interval": "D"}),
        ("Bybit", "BTC/USDC", BybitFetcher.fetch, {"interval": "D"}),
        ("OKX", "BTC/USDT", OKXFetcher.fetch, {"interval": "1D"}),
        ("OKX", "BTC/USDC", OKXFetcher.fetch, {"interval": "1D"}),
        ("Coinbase", "BTC/USD", CoinbaseFetcher.fetch, {"granularity_sec": 86400}),
        ("Crypto.com", "BTC/USDT", CryptoComFetcher.fetch, {"interval": "1D"}),
        ("Crypto.com", "BTC/USDC", CryptoComFetcher.fetch, {"interval": "1D"}),
        ("Hyperliquid", "BTC/USDC", HyperliquidFetcher.fetch, {"interval": "1d"}),
        ("Upbit", "BTC/KRW", UpbitFetcher.fetch_days, {}),
        ("Bitget", "BTC/USDT", BitgetFetcher.fetch, {"interval": "1D"}),
        ("Bitget", "BTC/USDC", BitgetFetcher.fetch, {"interval": "1D"}),
        ("MEXC", "BTC/USDT", MEXCFetcher.fetch, {"interval": "1d"}),
        ("MEXC", "BTC/USDC", MEXCFetcher.fetch, {"interval": "1d"}),
    ]
    
    data: Dict[str, pd.DataFrame] = {}
    named_usd_datasets: List[Tuple[str, pd.DataFrame]] = []
    
    data: Dict[str, pd.DataFrame] = {}
    named_usd_datasets: List[Tuple[str, pd.DataFrame]] = []
    
    # 1. First Pass: Determine which dates need recalculation
    print("\n=== Checking Database Status ===")
    recalc_start_date = datetime.now(timezone.utc)
    fetch_plan = []
    
    for exch, sym, fn, kwargs in tasks:
        last_ts = get_last_timestamp(exch, sym)
        if last_ts is not None:
            # Re-fetch from 1 day before last timestamp to finalize partial candles
            start_date = last_ts - timedelta(days=1)
            recalc_start_date = min(recalc_start_date, start_date)
            fetch_plan.append((exch, sym, fn, kwargs, start_date))
        else:
            recalc_start_date = min(recalc_start_date, datetime.fromtimestamp(default_start_ms / 1000, tz=timezone.utc))
            fetch_plan.append((exch, sym, fn, kwargs, None))

    recalc_start_ms = to_ms(recalc_start_date)
    print(f"Global recalculation starts from: {recalc_start_date.date()}")

    # 2. Second Pass: Fetch new data or load recent history from DB
    print("\n=== Syncing Exchange Data ===")
    for exch, sym, fn, kwargs, start_date in fetch_plan:
        key = f"{exch}:{sym}"
        
        if start_date:
            days_behind = (datetime.now(timezone.utc) - start_date).days
            print(f"\n[{exch}] {sym}: Fetching {days_behind} days (from {start_date.date()})...")
            
            try:
                # Always limit fetch to start_date
                kwargs["start_ms"] = to_ms(start_date)
                df = fn(sym, **kwargs)
                data[key] = df
                
                if df is not None and not df.empty:
                    upsert_ohlcv(df, exch, sym)
                    print(f"[{exch}] {sym} -> {len(df)} new/updated rows")
                    
                    # Track USD-like pairs for aggregation (exclude Upbit KRW)
                    if (sym.endswith("/USD") or sym.endswith("/USDT") or sym.endswith("/USDC")) and exch != "Upbit":
                        name = f"{exch} {sym}"
                        if exch == "Hyperliquid": name = "Hyperliquid BTC/USD"
                        named_usd_datasets.append((name, df))
            except Exception as e:
                print(f"[{exch}] Error fetching {sym}: {e}")
        else:
            # First run for this exchange
            print(f"\n[{exch}] {sym}: No existing data, fetching full history...")
            try:
                kwargs["start_ms"] = default_start_ms
                df = fn(sym, **kwargs)
                data[key] = df
                if df is not None and not df.empty:
                    upsert_ohlcv(df, exch, sym)
                    if (sym.endswith("/USD") or sym.endswith("/USDT") or sym.endswith("/USDC")) and exch != "Upbit":
                        name = f"{exch} {sym}"
                        if exch == "Hyperliquid": name = "Hyperliquid BTC/USD"
                        named_usd_datasets.append((name, df))
            except Exception as e:
                print(f"[{exch}] Error fetching {sym}: {e}")

    # 3. Third Pass: For all exchanges, ensure we have the RECENT history for overlapping calculation
    # (e.g., if we fetched MEXC but Binance was up to date, we still need Binance for the same dates)
    print("\n=== Loading Missing Overlap Data from DB ===")
    for exch, sym, _, _ in tasks:
        key = f"{exch}:{sym}"
        # If we already have enough data from the fetch, skip
        if key in data and not data[key].empty and data[key]['timestamp'].min() <= recalc_start_date:
            continue
            
        try:
            conn = get_db_connection()
            # ONLY load from recalc_start_date onwards
            df = pd.read_sql("""
                SELECT o.timestamp, o.open, o.high, o.low, o.close, o.volume,
                        e.name as exchange, s.name as symbol
                FROM ohlcv_daily o
                JOIN exchanges e ON o.exchange_id = e.id
                JOIN symbols s ON o.symbol_id = s.id
                WHERE e.name = %s AND s.name = %s AND o.timestamp >= %s
                ORDER BY timestamp
            """, conn, params=(exch, sym, recalc_start_date))
            conn.close()
            
            if not df.empty:
                # Merge with existing fetched data if any
                if key in data and not data[key].empty:
                    df = pd.concat([df, data[key]], ignore_index=True).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                
                data[key] = df
                if (sym.endswith("/USD") or sym.endswith("/USDT") or sym.endswith("/USDC")) and exch != "Upbit":
                    name = f"{exch} {sym}"
                    if exch == "Hyperliquid": name = "Hyperliquid BTC/USD"
                    # Add to aggregation list if not already there or update it
                    named_usd_datasets = [d for d in named_usd_datasets if d[0] != name]
                    named_usd_datasets.append((name, df))
        except Exception as e:
            print(f"[{exch}] Error loading overlap from DB: {e}")

    # 4. Fourth Pass: Filter everything to RECALC window to ensure consistency
    print(f"\n=== Filtering data to window: {recalc_start_date.date()} onwards ===")
    
    # Filter 'data' dictionary
    for key in data:
        if data[key] is not None and not data[key].empty:
            data[key] = data[key][data[key]["timestamp"] >= recalc_start_date].copy()
            
    # Filter 'named_usd_datasets'
    filtered_usd_datasets = []
    for name, df in named_usd_datasets:
        if df is not None and not df.empty:
            df_filtered = df[df["timestamp"] >= recalc_start_date].copy()
            if not df_filtered.empty:
                filtered_usd_datasets.append((name, df_filtered))
    named_usd_datasets = filtered_usd_datasets

    # Get date range for FX
    all_dfs = [df for df in data.values() if df is not None and not df.empty]
    if not all_dfs:
        print("No data in current recalculation window!")
        return
    
    # Fetch FX rates starting exactly from our window
    print("\n=== Fetching FX Rates ===")
    fx_start = recalc_start_date.date().isoformat()
    fx_end = datetime.now(timezone.utc).date().isoformat()
    fx_df = fetch_usdkrw_timeseries(fx_start, fx_end)
    
    # Calculate Coinbase Premium
    print("\n=== Calculating Coinbase Premium ===")
    coin_df = data.get("Coinbase:BTC/USD")
    refs_ex_coin = [df for name, df in named_usd_datasets if not name.startswith("Coinbase")]
    cb_prem = compute_coinbase_premium(coin_df, refs_ex_coin)
    if not cb_prem.empty:
        print(f"Coinbase premium: {len(cb_prem)} days")
    
    # Build USD reference for Kimchi
    print("\n=== Calculating Kimchi Premium ===")
    ref_usd_df = pd.DataFrame()
    if refs_ex_coin:
        parts = []
        for df in refs_ex_coin:
            dd = ensure_daily(df)[["date", "close", "volume", "exchange"]].copy()
            dd["w_usd"] = dd["close"] * dd["volume"]
            dd.loc[dd["exchange"].str.contains("Hyperliquid", case=False, na=False), "w_usd"] *= HYPERLIQUID_WEIGHT_PENALTY
            parts.append(dd)
        if parts:
            ref_concat = pd.concat(parts, ignore_index=True)
            ref_agg = ref_concat.groupby("date").apply(lambda g: pd.Series({
                "close": (g["close"] * g["w_usd"]).sum() / max(g["w_usd"].sum(), 1e-12)
            }), include_groups=False).reset_index()
            ref_agg["timestamp"] = pd.to_datetime(ref_agg["date"], utc=True)
            ref_usd_df = ref_agg
    
    upbit_df = data.get("Upbit:BTC/KRW", pd.DataFrame())
    kimchi_prem = compute_kimchi_premium(upbit_df, ref_usd_df, fx_df)
    if not kimchi_prem.empty:
        print(f"Kimchi premium: {len(kimchi_prem)} days")
    
    # Save premiums
    upsert_premiums(cb_prem, kimchi_prem)
    
    # Aggregate USD candles
    print("\n=== Aggregating USD Candles ===")
    usd_dfs = [df for _, df in named_usd_datasets]
    agg = aggregate_usd_candles(usd_dfs)
    upsert_aggregated(agg)
    
    # Compute weights
    print("\n=== Computing Exchange Weights ===")
    weights = compute_daily_weights(named_usd_datasets)
    upsert_weights(weights)
    
    # Export to CSV (if enabled)
    export_to_csv(data, agg, weights, cb_prem, kimchi_prem)
    
    # Print summary
    if not weights.empty:
        last_day = weights["date"].max()
        w_last = weights[weights["date"] == last_day][["name", "weight"]].sort_values("weight", ascending=False)
        print("\nLatest weights:")
        for _, row in w_last.iterrows():
            print(f"  {row['name']}: {row['weight']*100:.2f}%")
    
    print("\n" + "=" * 60)
    print("BTC Spot Data Pipeline - Complete")
    print(f"CSV Export: {'ENABLED' if EXPORT_CSV else 'DISABLED'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
