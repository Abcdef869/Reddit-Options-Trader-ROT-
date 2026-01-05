from __future__ import annotations

import contextlib
import io
import json
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yfinance as yf

# Map common text aliases -> Yahoo symbols
ALIAS_MAP: Dict[str, str] = {
    "SPX": "^GSPC",
    "SP500": "^GSPC",
    "SPXW": "^GSPC",
    "TSMC": "TSM",
}

# Tokens that are almost always NOT equities (filter out early)
NON_EQUITY_TOKENS = {
    "USD", "EUR", "GBP", "JPY", "CNY",
    "AI", "DD", "YOLO", "WSB", "IMO", "CEO", "CPI", "GDP", "FOMC",
    "US", "EU", "UK", "IRA", "SEC", "DOJ", "NATO", "BRICS", "PLA",
}

def _jsonable(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(x) for x in obj]
    return obj

@contextlib.contextmanager
def _quiet_yfinance():
    # yfinance prints a lot to stdout/stderr; swallow it.
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        yield


class MarketEnricher:
    """
    Lightweight yfinance market metadata enrichment.

    Writes a cache to storage/market_cache.json to avoid repeated calls.
    """
    def __init__(self, cache_path: str = "storage/market_cache.json", ttl_s: int = 3600) -> None:
        self.cache_path = Path(cache_path)
        self.ttl_s = ttl_s
        self._cache: Dict[str, Any] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        if self.cache_path.exists():
            try:
                self._cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
            except Exception:
                self._cache = {}
        else:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache = {}

    def _save_cache(self) -> None:
        try:
            self.cache_path.write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # cache failures should never break the pipeline
            pass

    def _fresh(self, sym: str) -> Optional[Dict[str, Any]]:
        entry = self._cache.get(sym)
        if not isinstance(entry, dict):
            return None
        ts = entry.get("ts")
        if not isinstance(ts, (int, float)):
            return None
        if (time.time() - ts) <= self.ttl_s:
            data = entry.get("data")
            if isinstance(data, dict):
                return data
        return None

    def _fetch(self, sym: str) -> Dict[str, Any]:
        # Try to get minimal price + 1d change.
        out: Dict[str, Any] = {"symbol": sym}

        with _quiet_yfinance():
            t = yf.Ticker(sym)

            # Price series
            try:
                hist = t.history(period="5d", interval="1d")
                if hist is not None and len(hist) > 0:
                    close = float(hist["Close"].iloc[-1])
                    out["last_close"] = close
                    if len(hist) >= 2:
                        prev = float(hist["Close"].iloc[-2])
                        out["pct_1d"] = (close / prev - 1.0) if prev else None
            except Exception as e:
                out["price_error"] = str(e)

            # Light fundamentals (can be slow / flaky)
            try:
                info = getattr(t, "fast_info", None)
                if isinstance(info, dict):
                    # fast_info keys vary; keep it optional
                    out["currency"] = info.get("currency")
                    out["last_price"] = info.get("lastPrice") or info.get("last_price")
                    out["market_cap"] = info.get("marketCap") or info.get("market_cap")
            except Exception:
                pass

        return out

    def get_symbol(self, raw: str) -> Optional[str]:
        s = raw.upper().strip()
        s = ALIAS_MAP.get(s, s)
        if s in NON_EQUITY_TOKENS:
            return None
        if len(s) <= 1:
            return None
        return s

    def enrich_symbols(self, symbols: list[str]) -> Dict[str, Any]:
        market: Dict[str, Any] = {}
        now = int(time.time())

        for raw in symbols:
            sym = self.get_symbol(raw)
            if not sym:
                continue

            cached = self._fresh(sym)
            if cached is not None:
                market[sym] = cached
                continue

            data = self._fetch(sym)
            market[sym] = data
            self._cache[sym] = {"ts": now, "data": data}

        self._save_cache()
        return market

    def enrich_event(self, event: Any) -> Any:
        """
        Mutates event.meta in-place (keeps your pipeline simple).
        Expected event has: event.entities (list[str]) and event.meta (dict).
        """
        entities = getattr(event, "entities", []) or []
        meta = getattr(event, "meta", None)
        if meta is None or not isinstance(meta, dict):
            meta = {}
            try:
                setattr(event, "meta", meta)
            except Exception:
                # If it's frozen, we'll just return it untouched.
                return event

        meta.setdefault("market", {})
        meta["market"].update(self.enrich_symbols(list(entities)))
        return event
