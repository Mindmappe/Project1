#!/usr/bin/env python3
"""
High Turnover Strategy - Improved
- NaN-safe computations to avoid pandas RuntimeWarnings
- Robust CCXT symbol handling (uses exchange.market/safeSymbol when needed)
- Detailed structured logging for signals, order placement, order book snapshots
- Aligned with Quant Bootcamp v3.10.3 handbook (rate limiting, channels, ops)
"""

import json, time, threading, logging, queue, math, os, sys
from typing import Dict, Any, Optional, List, Tuple, Union
from datetime import datetime
from collections import deque

import requests, websocket
import pandas as pd
import numpy as np
import ccxt
import ta

# ---------------------------
# CONFIG
# ---------------------------
EMAIL = "ujjwal3654@gmail.com"
PASSWORD = "QuantBootcamp1$73"
VIRTUAL_SUBACCOUNT = "virtual_subaccount_ujjwal3654@gmail.com"
ACCOUNT_NAME = "skyfallokxsub2"

REST_API_BASE = "https://quant-bootcamp-api.goquant.io"
WS_API_URL = "wss://quant-bootcamp-api.goquant.io/ws/v1/virtual-subaccount"

PAIRS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT",
    "ADA/USDT", "AVAX/USDT", "MATIC/USDT", "DOT/USDT"
]

TARGET_DAILY_TURNOVER = 100000
MIN_TRADE_SIZE_USD = 50
MAX_TRADE_SIZE_USD = 500
CANDLE_TIMEFRAME = "1m"
CANDLE_LIMIT = 150

MAX_OPEN_POSITIONS = 6
STOP_LOSS_PCT = 0.5
TAKE_PROFIT_PCT = 0.75
SPREAD_BPS = 10
PLACE_THROTTLE_SEC = 1.05
SOFT_DRAWDOWN_PCT = 12.0
HARD_DRAWDOWN_PCT = 14.5
USE_TWAP_ABOVE_USD = 300

MARKET_DATA_EXCHANGE = "binance"

# Logging setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --------------- Utility logging helpers ---------------

def log_json(level: int, event: str, **kwargs):
    try:
        payload = {"event": event, **kwargs}
        msg = json.dumps(payload, default=str)
    except Exception:
        msg = f"{event} | {kwargs}"
    logging.log(level, msg)

# ---------------------------
# Balance Utilities
# ---------------------------
class BalanceHandler:
    @staticmethod
    def extract_aggregated_metrics(balance: Union[Dict, List]) -> Dict[str, Any]:
        if not balance:
            return {}
        try:
            if isinstance(balance, dict):
                return balance.get("aggregated_metrics", {})
            if isinstance(balance, list) and balance:
                entry = balance[0]
                if isinstance(entry, dict):
                    return entry.get("aggregated_metrics", {})
            return {}
        except Exception:
            logging.warning("Could not extract aggregated metrics from balance")
            return {}

    @staticmethod
    def get_nav(balance: Union[Dict, List], default: float = 1000.0) -> float:
        metrics = BalanceHandler.extract_aggregated_metrics(balance)
        try:
            return float(metrics.get("total_nav", default))
        except Exception:
            return float(default)

    @staticmethod
    def get_peak_nav(balance: Union[Dict, List], default: float = 1000.0) -> float:
        metrics = BalanceHandler.extract_aggregated_metrics(balance)
        try:
            return float(metrics.get("peak_nav", default))
        except Exception:
            return float(default)

# ---------------------------
# Enhanced Bootcamp Client
# ---------------------------
class BootcampClient:
    def __init__(self, rest_base: str, ws_url: str, email: str, password: str, vsub: str, account_name: str):
        self.rest_base = rest_base.rstrip("/")
        self.ws_url = ws_url
        self.email = email
        self.password = password
        self.vsub = vsub
        self.account_name = account_name

        self.access_token: Optional[str] = None
        self.token_expiry: float = 0.0

        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.connected = False
        self.inbound_q: "queue.Queue[Dict]" = queue.Queue()
        self.outbound_lock = threading.Lock()
        self.subscriptions = set()
        self.last_place_time = 0.0

        self.active_algos: Dict[str, Dict] = {}
        self.order_updates: Dict[str, Dict] = {}
        self.algorithm_updates: Dict[str, Dict] = {}

        self.daily_volume = 0.0
        self.daily_trades = 0
        self.daily_reset_date = datetime.utcnow().date()

        self.login()

    # -------- Auth ---------
    def login(self) -> bool:
        try:
            url = f"{self.rest_base}/auth/v2/validate_user"
            r = requests.post(url, json={"email": self.email, "password": self.password}, timeout=10)
            r.raise_for_status()
            j = r.json()
            access = j.get("data", {}).get("access_token")
            if not access:
                logging.error("Login failed: no access_token in response")
                return False
            self.access_token = access
            self.token_expiry = time.time() + (24 * 3600) - 60
            log_json(logging.INFO, "auth_success")
            return True
        except Exception as e:
            log_json(logging.ERROR, "auth_failed", error=str(e))
            return False

    def ensure_token(self):
        if not self.access_token or time.time() > self.token_expiry:
            logging.info("Refreshing token...")
            self.login()

    # -------- Websocket ---------
    def start_ws(self):
        self.ensure_token()
        if not self.access_token:
            raise RuntimeError("No access token for websocket")

        headers = [
            f"Authorization: Bearer {self.access_token}",
            f"Virtual-Subaccount-Name: {self.vsub}",
        ]

        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            header=headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.ws_thread = threading.Thread(target=self._ws_forever, daemon=True)
        self.ws_thread.start()
        time.sleep(1)

    def _ws_forever(self):
        while True:
            try:
                self.ws_app.run_forever(ping_interval=10, ping_timeout=5)
            except Exception as e:
                log_json(logging.ERROR, "ws_run_error", error=str(e))
            logging.info("WS reconnecting in 2s...")
            self.connected = False
            time.sleep(2)
            self.ensure_token()
            headers = [
                f"Authorization: Bearer {self.access_token}",
                f"Virtual-Subaccount-Name: {self.vsub}",
            ]
            self.ws_app = websocket.WebSocketApp(
                self.ws_url,
                header=headers,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

    def _on_open(self, ws):
        self.connected = True
        log_json(logging.INFO, "ws_connected")
        for channel in ("orders", "algorithms"):
            self.subscribe(channel)

    def _on_message(self, ws, msg):
        try:
            pkt = json.loads(msg)
            self.inbound_q.put(pkt)
            if "algorithm_place_response" in pkt:
                resp = pkt["algorithm_place_response"]
                cid = str(resp.get("client_algo_id"))
                if cid:
                    self.active_algos[cid] = {"status": resp.get("status", "placed"), "ts": time.time()}
                    log_json(logging.INFO, "algo_place_response", **resp)
            if "orders" in pkt:
                ords = pkt["orders"]
                cid = str(ords.get("client_algo_id", ""))
                if cid:
                    self.order_updates[cid] = ords
                    log_json(logging.INFO, "order_update", **ords)
            if "algorithms" in pkt:
                alg = pkt["algorithms"]
                cid = str(alg.get("client_algo_id", ""))
                if cid:
                    self.algorithm_updates[cid] = alg
                    log_json(logging.INFO, "algorithm_update", **alg)
                    state = alg.get("state", "")
                    if state in ("completed", "cancelled", "failed"):
                        self.active_algos.pop(cid, None)
        except Exception as e:
            log_json(logging.ERROR, "ws_message_parse_error", error=str(e), raw=msg)

    def _on_error(self, ws, err):
        log_json(logging.ERROR, "ws_error", error=str(err))

    def _on_close(self, ws, code, reason):
        self.connected = False
        log_json(logging.WARNING, "ws_closed", code=code, reason=reason)

    def send_ws(self, payload: Dict) -> bool:
        if not self.connected or not self.ws_app:
            return False
        try:
            with self.outbound_lock:
                self.ws_app.send(json.dumps(payload))
            log_json(logging.DEBUG, "ws_send", **payload)
            return True
        except Exception as e:
            log_json(logging.ERROR, "ws_send_failed", error=str(e), payload=payload)
            return False

    def subscribe(self, channel: str):
        if channel in self.subscriptions:
            return
        if self.send_ws({"op": "subscribe", "channel": channel}):
            self.subscriptions.add(channel)
            log_json(logging.INFO, "subscribed", channel=channel)

    def _wait_for_response(self, msg_type: str, timeout: float = 3.0) -> Optional[Dict]:
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                pkt = self.inbound_q.get(timeout=0.2)
                if msg_type in pkt:
                    return pkt[msg_type]
            except queue.Empty:
                continue
        return None

    # -------- Operations (place/cancel/balance) ---------
    def _throttle(self):
        now = time.time()
        delta = now - self.last_place_time
        if delta < PLACE_THROTTLE_SEC:
            time.sleep(PLACE_THROTTLE_SEC - delta)
        self.last_place_time = time.time()

    def place_market_edge(self, base: str, quote: str, side: str, quantity: float, duration: int = 10) -> Dict:
        payload = {
            "op": "place",
            "algorithm_type": "market_edge",
            "exchange_name": "okx",
            "account_name": self.account_name,
            "base": base,
            "quote": quote,
            "side": side,
            "quantity": float(max(quantity, 0.0)),
            "duration": float(duration),
        }
        self._throttle()
        self.send_ws(payload)
        resp = self._wait_for_response("algorithm_place_response", 3.0) or {"status": "submitted"}
        log_json(logging.INFO, "place_market_edge", request=payload, response=resp)
        return resp

    def place_limit_edge(self, base: str, quote: str, side: str, quantity: float, duration: int = 30, price: Optional[float] = None, threshold: float = 0.05) -> Dict:
        payload = {
            "op": "place",
            "algorithm_type": "limit_edge",
            "exchange_name": "okx",
            "account_name": self.account_name,
            "base": base,
            "quote": quote,
            "side": side,
            "quantity": float(max(quantity, 0.0)),
            "duration": float(duration),
            "threshold": float(max(threshold, 0.0)),
        }
        if price is not None and price > 0:
            payload["price"] = float(price)
        self._throttle()
        self.send_ws(payload)
        resp = self._wait_for_response("algorithm_place_response", 3.0) or {"status": "submitted"}
        log_json(logging.INFO, "place_limit_edge", request=payload, response=resp)
        return resp

    def place_twap_edge(self, base: str, quote: str, side: str, quantity: float, duration: int = 60, interval: int = 5) -> Dict:
        payload = {
            "op": "place",
            "algorithm_type": "twap_edge",
            "exchange_name": "okx",
            "account_name": self.account_name,
            "base": base,
            "quote": quote,
            "side": side,
            "quantity": float(max(quantity, 0.0)),
            "duration": float(duration),
            "interval": int(max(interval, 1)),
        }
        self._throttle()
        self.send_ws(payload)
        resp = self._wait_for_response("algorithm_place_response", 3.0) or {"status": "submitted"}
        log_json(logging.INFO, "place_twap_edge", request=payload, response=resp)
        return resp

    def cancel_algorithm(self, client_algo_id: str) -> Dict:
        payload = {"op": "cancel", "client_algo_id": str(client_algo_id)}
        self.send_ws(payload)
        resp = self._wait_for_response("algorithm_cancel_response", 3.0) or {"status": "cancel_submitted"}
        log_json(logging.INFO, "cancel_algorithm", request=payload, response=resp)
        return resp

    def fetch_balance(self) -> Union[Dict, List]:
        payload = {"op": "virtual_subaccount_balance"}
        self.send_ws(payload)
        resp = self._wait_for_response("virtual_subaccount_balance", 3.0) or {}
        log_json(logging.DEBUG, "fetch_balance_response", response=resp)
        return resp

    # -------- Daily stats ---------
    def _track_trade(self, quantity: float, price: float):
        today = datetime.utcnow().date()
        if today != self.daily_reset_date:
            self.daily_volume = 0.0
            self.daily_trades = 0
            self.daily_reset_date = today
        self.daily_trades += 1
        if price and price > 0:
            self.daily_volume += float(quantity) * float(price)

    def get_daily_stats(self) -> Tuple[float, int]:
        return self.daily_volume, self.daily_trades

# ---------------------------
# Market Data Manager with CCXT (NaN-safe)
# ---------------------------
class MarketDataManager:
    def __init__(self, exchange_name: str = MARKET_DATA_EXCHANGE):
        self.exchange = getattr(ccxt, exchange_name)({"enableRateLimit": True})
        self.exchange.load_markets()
        self.cache: Dict[str, pd.DataFrame] = {}
        self.cache_expiry: Dict[str, float] = {}
        self.cache_duration = 5.0
        self.lock = threading.Lock()
        logging.info(f"Market data initialized with {exchange_name}")

    def _to_exchange_symbol(self, symbol: str) -> str:
        try:
            if symbol in self.exchange.markets:
                return self.exchange.markets[symbol]["symbol"]
            # Try safeSymbol to normalize
            base, quote = symbol.split("/")
            synthetic = f"{base}/{quote}"
            return self.exchange.safe_symbol(synthetic)
        except Exception:
            return symbol.replace("/", "")

    def get_ohlcv(self, symbol: str, timeframe: str = CANDLE_TIMEFRAME, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
        cache_key = f"{symbol}_{timeframe}"
        with self.lock:
            if cache_key in self.cache and time.time() < self.cache_expiry.get(cache_key, 0):
                return self.cache[cache_key].copy()
            try:
                market_symbol = self._to_exchange_symbol(symbol)
                ohlcv = self.exchange.fetch_ohlcv(market_symbol, timeframe=timeframe, limit=limit)
                if not ohlcv:
                    raise ValueError("empty_ohlcv")
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])\
                        .dropna(how="any")
                if df.empty:
                    raise ValueError("empty_df_after_dropna")
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
                df = df.dropna(subset=["timestamp"]).set_index("timestamp")
                # Ensure numeric and replace non-finite (avoid deprecated fillna(method=...))
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = (
                        pd.to_numeric(df[col], errors="coerce")
                        .replace([np.inf, -np.inf], np.nan)
                        .ffill()
                        .bfill()
                    )
                # Log sanitization status
                bad_vals = int(df[["open","high","low","close","volume"]].isna().sum().sum())
                log_json(logging.DEBUG, "ohlcv_sanitized", symbol=symbol, rows=len(df), bad_values=bad_vals)
                self.cache[cache_key] = df
                self.cache_expiry[cache_key] = time.time() + self.cache_duration
                return df.copy()
            except Exception as e:
                log_json(logging.WARNING, "ohlcv_fetch_failed", symbol=symbol, error=str(e))
                return self.cache.get(cache_key, pd.DataFrame()).copy()

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        try:
            market_symbol = self._to_exchange_symbol(symbol)
            t = self.exchange.fetch_ticker(market_symbol)
            return t or {}
        except Exception as e:
            log_json(logging.WARNING, "ticker_fetch_failed", symbol=symbol, error=str(e))
            return {}

    def get_order_book(self, symbol: str, limit: int = 10) -> Dict[str, Any]:
        try:
            market_symbol = self._to_exchange_symbol(symbol)
            ob = self.exchange.fetch_order_book(market_symbol, limit=limit)
            # Trim and sanitize
            bids = [[float(b[0]), float(b[1])] for b in (ob.get("bids") or [])[:limit] if len(b) >= 2]
            asks = [[float(a[0]), float(a[1])] for a in (ob.get("asks") or [])[:limit] if len(a) >= 2]
            snapshot = {"bids": bids, "asks": asks, "ts": ob.get("timestamp")}
            log_json(logging.DEBUG, "order_book_snapshot", symbol=symbol, top_bid=bids[0] if bids else None, top_ask=asks[0] if asks else None)
            return snapshot
        except Exception as e:
            log_json(logging.WARNING, "order_book_fetch_failed", symbol=symbol, error=str(e))
            return {"bids": [], "asks": []}

# ---------------------------
# High-Frequency Trading Engine (NaN-safe signals + rich logs)
# ---------------------------
class HighFrequencyEngine:
    def __init__(self, client: BootcampClient):
        self.client = client
        self.market_data = MarketDataManager()
        self.positions: Dict[str, Dict] = {}
        self.price_cache: Dict[str, float] = {}
        self.volatility_cache: Dict[str, float] = {}
        self.momentum_cache: Dict[str, float] = {}

        self.nav = 1000.0
        self.peak_nav = 1000.0
        self.last_rebalance = time.time()

        self.signal_queue: "queue.Queue[Dict]" = queue.Queue()
        self.execution_thread = threading.Thread(target=self._execution_loop, daemon=True)
        self.execution_thread.start()
        self.market_thread = threading.Thread(target=self._market_data_loop, daemon=True)
        self.market_thread.start()

    # ---------- Signal calculation ----------
    def _nan_to_zero(self, value: float) -> float:
        try:
            if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                return 0.0
            if isinstance(value, (np.floating, np.float32, np.float64)) and (np.isnan(value) or np.isinf(value)):
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    def _calculate_signals(self, symbol: str) -> Dict[str, float]:
        signals: Dict[str, float] = {}
        df = self.market_data.get_ohlcv(symbol, CANDLE_TIMEFRAME, CANDLE_LIMIT)
        if df.empty or len(df) < 30:
            return {}

        ticker = self.market_data.get_ticker(symbol)
        current_price = float(ticker.get("last") or df["close"].iloc[-1])
        if not np.isfinite(current_price) or current_price <= 0:
            return {}

        close_series = df["close"].astype(float)
        # Momentum
        sma_fast = close_series.rolling(5, min_periods=5).mean().iloc[-1]
        sma_slow = close_series.rolling(20, min_periods=20).mean().iloc[-1]
        momentum = 0.0
        if np.isfinite(sma_fast) and np.isfinite(sma_slow) and sma_slow > 0:
            momentum = (sma_fast - sma_slow) / sma_slow * 100.0
        signals["momentum"] = self._nan_to_zero(momentum)

        # Bollinger mean reversion
        bb_period = 20
        ma = close_series.rolling(bb_period, min_periods=bb_period).mean()
        std = close_series.rolling(bb_period, min_periods=bb_period).std()
        bb_upper = ma + (2.0 * std)
        bb_lower = ma - (2.0 * std)
        mean_rev = 0
        if np.isfinite(bb_upper.iloc[-1]) and np.isfinite(bb_lower.iloc[-1]):
            if current_price > float(bb_upper.iloc[-1]):
                mean_rev = -1
            elif current_price < float(bb_lower.iloc[-1]):
                mean_rev = 1
        signals["mean_reversion"] = mean_rev

        # RSI
        try:
            rsi_val = float(ta.momentum.RSIIndicator(close_series, window=14).rsi().iloc[-1])
            if not np.isfinite(rsi_val):
                rsi_val = 50.0
        except Exception:
            rsi_val = 50.0
        rsi_sig = 0
        if rsi_val > 70:
            rsi_sig = -1
        elif rsi_val < 30:
            rsi_sig = 1
        signals["rsi"] = rsi_sig

        # Volume spike
        vol_series = df["volume"].astype(float)
        vol_ma = vol_series.rolling(20, min_periods=20).mean().iloc[-1]
        vol_spike = 0.0
        if np.isfinite(vol_ma) and vol_ma > 0:
            vol_spike = (float(vol_series.iloc[-1]) - float(vol_ma)) / float(vol_ma)
        signals["volume_spike"] = self._nan_to_zero(vol_spike)

        # Volatility regime: use log returns
        try:
            returns = np.diff(np.log(close_series.values))
            last20 = returns[-20:] if len(returns) >= 20 else returns
            vol_annualized_pct = np.nanstd(last20) * np.sqrt(1440) * 100.0
            if not np.isfinite(vol_annualized_pct):
                vol_annualized_pct = 0.0
        except Exception:
            vol_annualized_pct = 0.0
        signals["volatility"] = float(vol_annualized_pct)

        # VWAP deviation over the window
        vol_sum = float(df["volume"].sum())
        vwap = (float((df["close"] * df["volume"]).sum()) / vol_sum) if vol_sum > 0 else current_price
        vwap_dev = ((current_price - vwap) / vwap * 100.0) if vwap > 0 else 0.0
        signals["vwap_deviation"] = self._nan_to_zero(vwap_dev)

        # Order book imbalance
        book = self.market_data.get_order_book(symbol, 5)
        ob_imb = 0.0
        try:
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid_vol = float(sum([b[1] for b in bids[:5]]))
            ask_vol = float(sum([a[1] for a in asks[:5]]))
            tot = bid_vol + ask_vol
            if tot > 0:
                ob_imb = (bid_vol - ask_vol) / tot
        except Exception:
            ob_imb = 0.0
        signals["order_book_imbalance"] = self._nan_to_zero(ob_imb)

        # Cache
        self.price_cache[symbol] = float(current_price)
        self.volatility_cache[symbol] = float(signals["volatility"])
        self.momentum_cache[symbol] = float(signals["momentum"])

        # Structured log of signals
        log_json(logging.INFO, "signals", symbol=symbol, price=current_price, **signals)
        return signals

    # ---------- Trade generation ----------
    def _generate_trades(self, symbol: str, signals: Dict[str, float]):
        if not signals:
            return
        current_price = self.price_cache.get(symbol, 0.0)
        if not current_price or current_price <= 0:
            return
        if not self._check_risk_limits():
            return

        strength = 0.0
        count = 0
        if abs(signals.get("momentum", 0.0)) > 0.1:
            strength += signals["momentum"] * 0.25
            count += 1
        if signals.get("mean_reversion", 0) != 0:
            strength += signals["mean_reversion"] * 0.3
            count += 1
        if signals.get("rsi", 0) != 0:
            strength += signals["rsi"] * 0.2
            count += 1
        if signals.get("volume_spike", 0.0) > 0.5:
            strength += 0.15
            count += 1
        if abs(signals.get("vwap_deviation", 0.0)) > 0.5:
            strength += math.copysign(0.1, signals["vwap_deviation"])
            count += 1
        if abs(signals.get("order_book_imbalance", 0.0)) > 0.2:
            strength += signals["order_book_imbalance"] * 0.15
            count += 1
        if count:
            strength /= count
        if abs(strength) < 0.15:
            return

        side = "buy" if strength > 0 else "sell"
        confidence = min(abs(strength) * 2.0, 1.0)
        volatility = self.volatility_cache.get(symbol, 1.0)
        base_size = MIN_TRADE_SIZE_USD + (MAX_TRADE_SIZE_USD - MIN_TRADE_SIZE_USD) * confidence
        if volatility < 1.0:
            size_usd = base_size * 1.5
        elif volatility > 3.0:
            size_usd = base_size * 0.7
        else:
            size_usd = base_size
        size_usd = float(max(MIN_TRADE_SIZE_USD, min(MAX_TRADE_SIZE_USD, size_usd)))
        quantity = size_usd / current_price

        algo_type = "limit"
        duration = 20
        interval = None
        if size_usd > USE_TWAP_ABOVE_USD:
            algo_type = "twap"
            duration = 30
            interval = 5
        elif volatility > 2.0 or abs(signals.get("volume_spike", 0.0)) > 1.0:
            algo_type = "market"
            duration = 10

        stop_loss = current_price * (1 - STOP_LOSS_PCT / 100.0) if side == "buy" else current_price * (1 + STOP_LOSS_PCT / 100.0)
        take_profit = current_price * (1 + TAKE_PROFIT_PCT / 100.0) if side == "buy" else current_price * (1 - TAKE_PROFIT_PCT / 100.0)

        trade = {
            "symbol": symbol,
            "side": side,
            "quantity": float(quantity),
            "size_usd": float(size_usd),
            "algo_type": algo_type,
            "duration": duration,
            "interval": interval,
            "stop_loss": float(stop_loss),
            "take_profit": float(take_profit),
            "confidence": float(confidence),
            "signal_strength": float(strength),
            "signals": signals,
            "timestamp": time.time(),
        }
        log_json(logging.INFO, "trade_signal", **trade)
        self.signal_queue.put(trade)

    # ---------- Execution ----------
    def _execution_loop(self):
        while True:
            try:
                trade = self.signal_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                if not self._check_risk_limits():
                    log_json(logging.WARNING, "trade_skipped_risk")
                    continue
                if len(self.positions) >= MAX_OPEN_POSITIONS:
                    log_json(logging.INFO, "trade_skipped_max_positions")
                    continue
                self._execute_trade(trade)
            except Exception as e:
                log_json(logging.ERROR, "execution_error", error=str(e))

    def _execute_trade(self, trade: Dict[str, Any]):
        symbol = trade["symbol"]
        base, quote = symbol.split("/")
        side = trade["side"]
        qty = float(max(trade["quantity"], 0.0))
        algo = trade["algo_type"]

        log_json(logging.INFO, "placing_order", symbol=symbol, side=side, quantity=qty, algo=algo, size_usd=trade["size_usd"])
        try:
            if algo == "market":
                resp = self.client.place_market_edge(base, quote, side, qty, duration=trade["duration"])
            elif algo == "limit":
                price_now = self.price_cache.get(symbol, 0.0)
                if side == "buy":
                    limit_price = price_now * (1 - SPREAD_BPS / 10000.0)
                else:
                    limit_price = price_now * (1 + SPREAD_BPS / 10000.0)
                resp = self.client.place_limit_edge(base, quote, side, qty, duration=trade["duration"], price=limit_price, threshold=0.02)
            else:
                resp = self.client.place_twap_edge(base, quote, side, qty, duration=trade["duration"], interval=trade["interval"] or 5)
            if resp and resp.get("status") != "failed":
                client_algo_id = str(resp.get("client_algo_id")) if resp.get("client_algo_id") is not None else None
                if client_algo_id:
                    self.positions[client_algo_id] = {
                        "symbol": symbol,
                        "side": side,
                        "quantity": qty,
                        "size_usd": trade["size_usd"],
                        "entry_price": self.price_cache.get(symbol, 0.0),
                        "stop_loss": trade["stop_loss"],
                        "take_profit": trade["take_profit"],
                        "timestamp": time.time(),
                    }
                    self.client._track_trade(qty, self.price_cache.get(symbol, 0.0))
            log_json(logging.INFO, "order_placed", response=resp)
        except Exception as e:
            log_json(logging.ERROR, "order_place_failed", error=str(e))

    # ---------- Market loop ----------
    def _market_data_loop(self):
        while True:
            try:
                for symbol in PAIRS:
                    try:
                        signals = self._calculate_signals(symbol)
                        if signals:
                            self._generate_trades(symbol, signals)
                    except Exception as e:
                        log_json(logging.WARNING, "symbol_process_error", symbol=symbol, error=str(e))
                self._check_positions()
                self._log_performance()
                time.sleep(3)
            except Exception as e:
                log_json(logging.ERROR, "market_loop_error", error=str(e))
                time.sleep(5)

    # ---------- Position/risk mgmt ----------
    def _check_positions(self):
        for algo_id, pos in list(self.positions.items()):
            symbol = pos["symbol"]
            price = self.price_cache.get(symbol, 0.0)
            if not price or price <= 0:
                continue
            stop_hit = (pos["side"] == "buy" and price <= pos["stop_loss"]) or (pos["side"] == "sell" and price >= pos["stop_loss"])
            tp_hit = (pos["side"] == "buy" and price >= pos["take_profit"]) or (pos["side"] == "sell" and price <= pos["take_profit"])
            age_hit = (time.time() - pos["timestamp"]) > 300
            if stop_hit:
                log_json(logging.WARNING, "stop_loss_trigger", algo_id=algo_id, symbol=symbol, price=price)
                self._close_position(algo_id)
            elif tp_hit:
                log_json(logging.INFO, "take_profit_trigger", algo_id=algo_id, symbol=symbol, price=price)
                self._close_position(algo_id)
            elif age_hit:
                log_json(logging.INFO, "close_aged_position", algo_id=algo_id, symbol=symbol)
                self._close_position(algo_id)

    def _close_position(self, algo_id: str):
        pos = self.positions.get(algo_id)
        if not pos:
            return
        symbol = pos["symbol"]
        base, quote = symbol.split("/")
        try:
            self.client.cancel_algorithm(algo_id)
        except Exception:
            pass
        close_side = "sell" if pos["side"] == "buy" else "buy"
        self.client.place_market_edge(base, quote, close_side, pos["quantity"], duration=10)
        del self.positions[algo_id]
        log_json(logging.INFO, "position_closed", algo_id=algo_id, symbol=symbol)

    def _check_risk_limits(self) -> bool:
        try:
            balance = self.client.fetch_balance()
            if not balance:
                return True
            current_nav = BalanceHandler.get_nav(balance, self.nav)
            peak_nav = BalanceHandler.get_peak_nav(balance, self.peak_nav)
            self.nav = current_nav
            self.peak_nav = max(self.peak_nav, peak_nav, current_nav)
            dd_pct = 0.0
            if self.peak_nav > 0:
                dd_pct = (self.peak_nav - self.nav) / self.peak_nav * 100.0
            log_json(logging.INFO, "risk_state", nav=self.nav, peak_nav=self.peak_nav, drawdown_pct=dd_pct)
            if dd_pct >= HARD_DRAWDOWN_PCT:
                log_json(logging.ERROR, "hard_drawdown_limit", drawdown_pct=dd_pct)
                return False
            return True
        except Exception as e:
            log_json(logging.WARNING, "risk_check_failed", error=str(e))
            return True

    # ---------- Reporting ----------
    def _log_performance(self):
        volume, trades = self.client.get_daily_stats()
        progress_pct = (volume / TARGET_DAILY_TURNOVER * 100.0) if TARGET_DAILY_TURNOVER > 0 else 0.0
        log_json(
            logging.INFO,
            "performance",
            nav=self.nav,
            daily_volume=volume,
            progress_pct=progress_pct,
            trades=trades,
            open_positions=len(self.positions),
            max_positions=MAX_OPEN_POSITIONS,
        )

    def run(self):
        logging.info("High-frequency engine started")
        logging.info(f"Target daily turnover: ${TARGET_DAILY_TURNOVER:,.0f}")
        logging.info(f"Trading {len(PAIRS)} pairs; max positions = {MAX_OPEN_POSITIONS}")
        while True:
            time.sleep(60)

# ---------------------------
# Main Entry Point
# ---------------------------

def main():
    logging.info("Starting High-Turnover Quant Bootcamp Strategy (Improved)")
    logging.info("Version: 3.10.3 | Target: $%d/day", TARGET_DAILY_TURNOVER)

    client = BootcampClient(REST_API_BASE, WS_API_URL, EMAIL, PASSWORD, VIRTUAL_SUBACCOUNT, ACCOUNT_NAME)
    client.start_ws()
    time.sleep(2)

    if not client.connected:
        logging.error("Failed to establish WebSocket connection")
        return

    balance = client.fetch_balance()
    if balance:
        nav = BalanceHandler.get_nav(balance)
        logging.info(f"Initial NAV: ${nav:.2f}")

    engine = HighFrequencyEngine(client)
    try:
        engine.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        for algo_id in list(engine.positions.keys()):
            try:
                engine._close_position(algo_id)
            except Exception:
                pass
        volume, trades = client.get_daily_stats()
        logging.info(f"Final daily volume: ${volume:.2f}")
        logging.info(f"Total trades: {trades}")

if __name__ == "__main__":
    main()