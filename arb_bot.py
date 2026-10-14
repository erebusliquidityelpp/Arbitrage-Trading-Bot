#!/usr/bin/env python3
"""
Automated arbitrage bot: cross-exchange price arb + funding-rate arb.

Both strategies trade PERPETUAL futures as a delta-neutral pair:
  long on exchange X, short on exchange Y, same coin, same notional.
No coins are moved between exchanges, only margin sits on each one.

  price   : enter when Y's bid is above X's ask by more than fees+buffer,
            exit when the gap converges (or stop / timeout).
  funding : enter when the funding paid to the short side (Y) exceeds
            what the long side (X) pays, by enough to cover fees,
            exit when the differential fades (or stop / timeout).

Default mode is PAPER (live prices, simulated fills). Live trading needs
BOTH  mode: live  in config.yaml  AND  --live --yes-real-money  on the CLI.

Usage:
  python arb_bot.py --demo --max-loops 300     # offline logic test
  python arb_bot.py                            # paper trading, live prices
  python arb_bot.py --live --yes-real-money    # real orders (read README first)
"""
import argparse
import calendar
import copy
import csv
import hmac
import json
import logging
import os
import random
import re
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dataclasses import asdict, dataclass

import gzip
import glob
import tempfile
import yaml

log = logging.getLogger("arb")

DEFAULTS = {
    "mode": "paper",
    "loop_seconds": 15,
    # "auto" = at startup, find EVERY perpetual that trades on all enabled exchanges.
    # Or give a manual list, e.g. [BTC, ETH, SOL].
    #   "liquid" = the top_liquid_n most-traded coins that exist on every exchange (ranked by 24h volume at startup)
    "bases": "liquid",
    "top_liquid_n": 20,
    "min_volume_usd": 50_000_000,   # a coin needs at least this 24h volume on EVERY exchange
    "liquid_fallback": ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "SUI", "LTC", "BCH",
                        "DOT", "NEAR", "AAVE", "UNI", "TRX", "HYPE", "ARB", "OP"],   # used only if volume data cannot be read
    "exclude_coins": [],          # coins to skip even if listed everywhere
    "min_exchanges": 2,           # coin must be listed on at least this many enabled exchanges
    "refine_top_n": 12,           # order-book check for the N most promising coins each scan
    "enabled_exchanges": ["binance", "hyperliquid"],
    # Gaps bigger than this are treated as data/contract-size errors, not opportunities.
    "max_price_gap_bps": 100.0,
    # taker_bps = taker fee in basis points (CHECK YOUR OWN FEE TIER)
    # funding_hours = funding interval of that venue
    # symbol_map = per-coin symbol overrides. null = skip this coin on that exchange
    # (used where the contract size / price scale differs, e.g. PEPE on OKX).
    "exchanges": {
        "binance": {"symbol": "{b}/USDT:USDT", "settle": "USDT", "taker_bps": 4.5, "funding_hours": 8,
                    "symbol_map": {"PEPE": "1000PEPE/USDT:USDT", "BONK": "1000BONK/USDT:USDT"}},
        "bybit": {"symbol": "{b}/USDT:USDT", "settle": "USDT", "taker_bps": 5.5, "funding_hours": 8,
                  "symbol_map": {"PEPE": "1000PEPE/USDT:USDT", "BONK": "1000BONK/USDT:USDT"}},
        "okx": {"symbol": "{b}/USDT:USDT", "settle": "USDT", "taker_bps": 5.0, "funding_hours": 8,
                "symbol_map": {"PEPE": None, "BONK": None}},
        "hyperliquid": {"symbol": "{b}/USDC:USDC", "settle": "USDC", "taker_bps": 4.5, "funding_hours": 1,
                        "symbol_map": {"PEPE": "kPEPE/USDC:USDC", "BONK": "kBONK/USDC:USDC"}},
    },
    "price_arb": {
        "enabled": True,
        # CONVERGENCE TRADE: every coin has a normal price gap between the two exchanges (its "basis"). The bot learns that
        # normal gap and only trades when the gap is stretched far beyond normal, then waits for it to snap back.
        "use_basis": True,
        "z_min": 3.0,                 # gap must be this many standard deviations away from its normal level
        "warmup_samples": 60,         # scans needed to learn a coin's normal gap before trading it (~15-20 min)
        "ewma_alpha": 0.005,          # how slowly the "normal gap" adapts (smaller = slower, ~200 scans)
        "min_sigma_bps": 0.7,         # floor for the normal wiggle, so a quiet coin cannot fake a huge z-score
        "min_net_edge_bps": 4.0,      # expected profit after ALL fees + buffer + spreads (entry and exit)
        "take_profit_bps": 3.0,       # close when net PnL >= this (bps of notional)
        "stop_loss_bps": 12.0,        # close when the gap moves this far against us AFTER entry
        "max_hold_minutes": 15,
        "max_spread_bps": 3.0,        # skip a coin if either exchange's bid/ask spread is wider than this
        "confirm_scans": 2,           # the same opportunity must appear this many scans in a row before entering
    },
    "funding_arb": {                  # REMOVED from the strategy: kept off and out of config.yaml
        "enabled": False,
        "min_apr_pct": 25.0,          # annualised funding differential needed
        "expected_hold_hours": 24,    # used to check funding covers fees
        "min_net_edge_bps": 5.0,
        "exit_apr_pct": 3.0,          # close when differential fades below this
        "min_hold_hours": 3.0,        # do not exit on "funding faded" before this long (funding is paid in hourly/8h steps; leaving earlier only pays fees)
        "confirm_scans": 4,           # the same funding opportunity must repeat this many scans in a row
        "max_hold_hours": 72,
        "stop_loss_bps": 25.0,
    },
    "record": {                       # saves every scan to disk so strategies can be replayed and tuned on REAL data
        "enabled": True,
        "dir": "/data/rec",
        "keep_days": 7,
    },
    "watchdog": {"enabled": True, "stale_seconds": 120},   # Telegram alert if no scan completes for this long
    "schedule": {
        "pause_weekends": True,       # no NEW trades on Saturday and Sunday (thin weekend liquidity)
        "timezone": "UTC",            # weekend is judged in this timezone, e.g. "Asia/Karachi"
    },
    "sizing": {
        "notional_usd": 100,          # per leg
        "leverage": 3,
        "max_open_positions": 3,
        "max_total_notional_usd": 300,
        "min_depth_multiple": 3,      # book must hold N x our size
    },
    "slippage_buffer_bps": 2.0,
    "paper_extra_slippage_bps": 1.0,
    "risk": {"daily_loss_limit_usd": 10, "cooldown_minutes": 30,
             "ban_after_losses": 2,     # a coin that loses this many trades in a row is banned ...
             "ban_hours": 24},          # ... for this long (stops re-entering the same failing coin)
    "files": {"state": "state.json", "trades": "trades.csv"},
}

FUNDING_REFRESH_S = 60


def deep_merge(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            deep_merge(a[k], v)
        else:
            a[k] = v
    return a


def load_config(path):
    cfg = copy.deepcopy(DEFAULTS)
    if path and os.path.exists(path):
        with open(path) as f:
            deep_merge(cfg, yaml.safe_load(f) or {})
    return cfg


def notify(msg):
    """Optional Telegram alert (env TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)."""
    tok, chat = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": msg}).encode()
        urllib.request.urlopen(
            urllib.request.Request(f"https://api.telegram.org/bot{tok}/sendMessage", data=data),
            timeout=10,
        )
    except Exception as e:  # never let alerts break trading
        log.warning("telegram failed: %s", e)


# --------------------------------------------------------------------------
# Market data
# --------------------------------------------------------------------------
def vwap(levels, notional_usd, contract_size=1.0):
    """Average price to fill notional_usd through book levels, or None if too thin."""
    need, cost, qty = notional_usd, 0.0, 0.0
    for price, amt in levels:
        take_usd = min(need, price * amt * contract_size)
        cost += take_usd
        qty += take_usd / price
        need -= take_usd
        if need <= 1e-9:
            break
    if need > 1e-6 or qty <= 0:
        return None
    return cost / qty


# Binance/Bybit write scaled coins as 1000PEPE / 1000000MOG / 1MBABYDOGE; Hyperliquid as kPEPE (= 1000x).
# 1INCH is a real coin, so only powers of ten >= 1000 count as a multiplier.
_NUM_PREFIX = re.compile(r"^(1000000000|100000000|10000000|1000000|100000|10000|1000)([A-Z].*)$")
_M_PREFIX = re.compile(r"^1M([A-Z]{3,})$")
_K_PREFIX = re.compile(r"^k([A-Z0-9].*)$")


def norm_base(base):
    """ccxt base currency -> (coin name, units-of-coin per contract unit). 'kPEPE' -> ('PEPE', 1000)."""
    m = _NUM_PREFIX.match(base)
    if m:
        return m.group(2), int(m.group(1))
    m = _M_PREFIX.match(base)
    if m:
        return m.group(1), 1_000_000
    m = _K_PREFIX.match(base)
    if m:
        return m.group(1), 1000
    return base, 1


def discover(markets_by_ex, settle_by_ex, exclude=(), min_exchanges=2):
    """Find every coin with a perpetual on at least `min_exchanges` of the exchanges.
    Returns ({coin: {exchange: (ccxt_symbol, multiplier)}}, [coins listed on only one exchange])."""
    per_ex = {}
    for name, markets in markets_by_ex.items():
        found = per_ex.setdefault(name, {})
        for m in markets.values():
            if not (m.get("swap") and m.get("linear")) or m.get("active") is False or m.get("expiry"):
                continue
            if m.get("settle") != settle_by_ex[name]:
                continue
            coin, mult = norm_base(m["base"])
            found.setdefault(coin, (m["symbol"], mult))
    coins, single = {}, []
    for coin in sorted({c for f in per_ex.values() for c in f}):
        if coin in exclude:
            continue
        where = {n: f[coin] for n, f in per_ex.items() if coin in f}
        if len(where) >= min_exchanges:
            coins[coin] = where
        else:
            single.append(coin)
    return coins, single


def parse_interval_h(txt, default):
    if txt:
        m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*h", str(txt))
        if m:
            return float(m.group(1))
    return default


def rank_by_volume(vols, exchanges, top_n, min_vol):
    """vols: {coin: {exchange: 24h volume in USD}}. A coin is only as liquid as its THINNEST exchange.
    Returns [(coin, thinnest_volume)] for the top_n coins at or above min_vol."""
    scored = []
    for coin, per in vols.items():
        vs = [per.get(n) for n in exchanges]
        if any(not v or v <= 0 for v in vs):
            continue
        m = min(vs)
        if m >= min_vol:
            scored.append((coin, m))
    scored.sort(key=lambda t: -t[1])
    return scored[:top_n]


def weekend_active(cfg, now=None):
    """True on Saturday and Sunday in the configured timezone, when schedule.pause_weekends is on."""
    sc = cfg.get("schedule") or {}
    if not sc.get("pause_weekends"):
        return False
    now = time.time() if now is None else now
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        wd = datetime.fromtimestamp(now, ZoneInfo(sc.get("timezone") or "UTC")).weekday()
    except Exception:
        wd = time.gmtime(now).tm_wday
    return wd >= 5


def gap_bps(qx, qy):
    """How much more expensive Y is than X, in bps, using mid prices."""
    mx, my = (qx["bid"] + qx["ask"]) / 2, (qy["bid"] + qy["ask"]) / 2
    return (my - mx) / mx * 1e4 if mx else None


class Basis:
    """Learns each coin's NORMAL price gap between every pair of exchanges (running mean and spread, in bps).
    The gap is stored for names in alphabetical order (x < y); stat() flips the sign for the other direction."""

    def __init__(self, alpha=0.005, state=None, max_gap=100.0):
        self.alpha, self.s, self.max_gap = alpha, (state if state is not None else {}), max_gap

    @staticmethod
    def key(base, x, y):
        return f"{base}|{x}|{y}"

    def update(self, snap):
        for base, per in snap.items():
            names = sorted(per)
            for i, x in enumerate(names):
                for y in names[i + 1:]:
                    g = gap_bps(per[x], per[y])
                    if g is None or abs(g) > self.max_gap:
                        continue
                    st = self.s.setdefault(self.key(base, x, y), {"mean": g, "var": 0.0, "n": 0})
                    n = st["n"]
                    a = max(self.alpha, 1.0 / (n + 1))
                    d = g - st["mean"]
                    if n >= 30:   # a violent spike must not drag the "normal" gap along with it
                        lim = 4 * max(st["var"] ** 0.5, 0.5)
                        d = max(-lim, min(lim, d))
                    st["mean"] += a * d
                    st["var"] = (1 - a) * (st["var"] + a * d * d)
                    st["n"] = n + 1

    def stat(self, base, x, y):
        """(normal gap of Y vs X in bps, normal wiggle in bps, samples) for the ordered pair (long x, short y)."""
        if x < y:
            st = self.s.get(self.key(base, x, y))
            sign = 1.0
        else:
            st = self.s.get(self.key(base, y, x))
            sign = -1.0
        if not st:
            return 0.0, 0.0, 0
        return sign * st["mean"], st["var"] ** 0.5, st["n"]

    def ready(self, base, warmup):
        return any(st["n"] >= warmup for k, st in self.s.items() if k.startswith(base + "|"))


class Feed:
    """Real market data via ccxt (public endpoints, no keys needed).

    Two-stage scan so that hundreds of coins stay inside the exchanges' rate limits:
      stage 1: ONE bulk call per exchange (mark price + funding for every coin)  -> rank all coins
      stage 2: full order books only for the top `refine_top_n` coins + coins with open positions
    Prices are normalised to "per 1 coin", so 1000PEPE and kPEPE compare correctly."""

    def __init__(self, cfg):
        import ccxt

        self.cfg = cfg
        self.ex, self.sym, self.cs, self.mult = {}, {}, {}, {}
        self.universe = 0
        self.basis = None          # set by the Bot: the learned "normal gap" per coin
        self.last_rough = {}       # mark prices of every tracked coin from the latest scan
        for name in cfg["enabled_exchanges"]:
            try:
                ex = getattr(ccxt, name)({"enableRateLimit": True})
                ex.load_markets()
            except Exception as e:
                log.warning("skipping %s: %s", name, e)
                continue
            self.ex[name] = ex
        if len(self.ex) < 2:
            raise SystemExit("Need at least 2 reachable exchanges.")

        bases = cfg["bases"]
        liquid = bases == "liquid" or bases == ["liquid"]
        auto_done = False
        if liquid or bases == "auto" or bases == ["auto"]:
            settle = {n: cfg["exchanges"][n].get("settle", "USDT") for n in self.ex}
            coins, single = discover({n: e.markets for n, e in self.ex.items()}, settle,
                                     set(cfg.get("exclude_coins") or []), cfg.get("min_exchanges", 2))
            for coin, where in coins.items():
                for n, (symbol, mult) in where.items():
                    self.sym[(n, coin)] = symbol
                    self.mult[(n, coin)] = mult
                    self.cs[(n, coin)] = self.ex[n].markets[symbol].get("contractSize") or 1.0
            cfg["bases"] = list(coins)
            auto_done = True
            log.info("AUTO coin list: %d coins trade on %s (%d more are listed on only one exchange)",
                     len(coins), " + ".join(self.ex), len(single))
            if liquid:
                ranked = self._rank_liquid(list(coins))
                if ranked:
                    keep = {c for c, _ in ranked}
                    for k in [k for k in self.sym if k[1] not in keep]:
                        self.sym.pop(k); self.mult.pop(k, None); self.cs.pop(k, None)
                    cfg["bases"] = [c for c, _ in ranked]
                    log.info("LIQUID coin list: %d coins (24h volume on the thinnest exchange): %s", len(ranked),
                             " ".join(f"{c}({v / 1e6:.0f}M)" for c, v in ranked))
                else:
                    log.warning("could not read 24h volumes: using the built-in liquid coin list instead")
                    self.sym, self.mult, self.cs = {}, {}, {}
                    bases = list(cfg.get("liquid_fallback") or [])
                    cfg["bases"] = bases
                    auto_done = False
            else:
                log.info("coins: %s", " ".join(coins))
        if not auto_done:  # manual list in config (uses symbol / symbol_map)
            for name, ex in self.ex.items():
                smap = cfg["exchanges"][name].get("symbol_map") or {}
                for b in bases:
                    s = smap[b] if b in smap else cfg["exchanges"][name]["symbol"].format(b=b)
                    if s and s in ex.markets:
                        self.sym[(name, b)] = s
                        self.cs[(name, b)] = ex.markets[s].get("contractSize") or 1.0
                        self.mult[(name, b)] = norm_base(ex.markets[s]["base"])[1]
                    elif s:
                        log.warning("%s has no market %s", name, s)
            dropped = [b for b in bases if sum(1 for (n, bb) in self.sym if bb == b) < 2]
            if dropped:
                log.warning("coins listed on <2 exchanges, will be ignored: %s", ", ".join(dropped))

    def _rank_liquid(self, coins):
        """Top coins by 24h USD volume on the thinnest exchange. Returns [] if volumes cannot be read."""
        vols = {}
        for n, ex in self.ex.items():
            syms = [self.sym[(n, c)] for c in coins if (n, c) in self.sym]
            try:
                tk = ex.fetch_tickers(syms)
            except Exception as e:
                log.warning("24h volume fetch failed on %s: %s", n, e)
                return []
            for c in coins:
                t = tk.get(self.sym.get((n, c))) or {}
                v = t.get("quoteVolume")
                if not v and t.get("baseVolume") and t.get("last"):
                    v = float(t["baseVolume"]) * float(t["last"])
                if v:
                    vols.setdefault(c, {})[n] = float(v)
        return rank_by_volume(vols, list(self.ex), int(self.cfg.get("top_liquid_n", 20)), float(self.cfg.get("min_volume_usd", 0)))

    def _rough(self, bases):
        """Stage 1: mark price + funding for all coins, one bulk call per exchange."""
        rough = {}
        for n, ex in self.ex.items():
            coins = [b for b in bases if (n, b) in self.sym]
            syms = [self.sym[(n, b)] for b in coins]
            try:
                rates = ex.fetch_funding_rates(syms)
            except Exception as e:
                log.warning("bulk funding/price fetch failed on %s: %s", n, e)
                rates = {}
            last = {}
            if not any((r.get("markPrice") or r.get("indexPrice")) for r in rates.values()):
                try:  # fallback: last traded prices
                    last = {k: (v.get("last") or v.get("close")) for k, v in ex.fetch_tickers(syms).items()}
                except Exception as e:
                    log.warning("ticker fallback failed on %s: %s", n, e)
            for b in coins:
                sym, r = self.sym[(n, b)], rates.get(self.sym[(n, b)]) or {}
                px = r.get("markPrice") or r.get("indexPrice") or last.get(sym)
                if not px:
                    continue
                px = float(px) / self.mult[(n, b)]
                fr = r.get("fundingRate")
                hrs = parse_interval_h(r.get("interval"), self.cfg["exchanges"][n]["funding_hours"])
                rough.setdefault(b, {})[n] = {"bid": px, "ask": px,
                                              "funding_h": float(fr) / hrs if fr is not None else None}
        return {b: per for b, per in rough.items() if len(per) >= 2}

    def snapshot(self, bases, notional, depth_mult, keep=()):
        rough = self._rough(bases)
        self.last_rough = rough
        self.universe = len(rough)
        if self.basis is not None:   # rank by how stretched the gap is versus the coin's own normal gap
            ranked = sorted(find_candidates(self.cfg, rough, self.basis, None, 0.5), key=lambda c: -c.z)
        else:
            ranked = sorted(find_candidates(self.cfg, rough), key=lambda c: -c.net_bps)
        refine = list(dict.fromkeys(list(keep) + [c.base for c in ranked]))  # open positions first
        top_n = int(self.cfg.get("refine_top_n", 12))
        refine = list(keep) + [b for b in refine if b not in keep][:top_n]

        jobs = [(n, b) for b in refine for n in self.ex if (n, b) in self.sym and b in rough and n in rough[b]]

        def work(j):
            n, b = j
            try:
                ob = self.ex[n].fetch_order_book(self.sym[(n, b)], 10)
                cs, mult = self.cs[(n, b)], self.mult[(n, b)]
                if not vwap(ob["asks"], notional * depth_mult, cs) or not vwap(ob["bids"], notional * depth_mult, cs):
                    return j, None
                return j, {
                    "ask": vwap(ob["asks"], notional, cs) / mult,
                    "bid": vwap(ob["bids"], notional, cs) / mult,
                    "funding_h": rough[b][n]["funding_h"],
                }
            except Exception as e:
                log.debug("book %s %s: %s", n, b, e)
                return j, None

        snap = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for (n, b), q in pool.map(work, jobs):
                if q:
                    snap.setdefault(b, {})[n] = q
        return snap


class MockFeed:
    """Synthetic data to test the logic offline (--demo)."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.basis = None
        self.last_rough = {}
        self.rng = random.Random(7)
        self.px = {"BTC": 65000.0, "ETH": 2600.0, "SOL": 150.0}
        self.fund = {}

    def snapshot(self, bases, notional, depth_mult, keep=()):
        snap = {}
        for b in bases:
            self.px.setdefault(b, 100.0)
            self.px[b] *= 1 + self.rng.gauss(0, 0.0003)
            for n in self.cfg["enabled_exchanges"]:
                off = self.rng.gauss(0, 2) + (self.rng.gauss(0, 25) if self.rng.random() < 0.10 else 0)
                mid = self.px[b] * (1 + off / 1e4)
                half = mid * 1.0 / 1e4
                f = self.fund.get((n, b), 0.00001)
                f += 0.05 * (0.00001 - f) + self.rng.gauss(0, 0.000012)
                self.fund[(n, b)] = f
                snap.setdefault(b, {})[n] = {"bid": mid - half, "ask": mid + half, "funding_h": f}
        self.last_rough = {b: {n: {"bid": (q["bid"] + q["ask"]) / 2, "ask": (q["bid"] + q["ask"]) / 2, "funding_h": q["funding_h"]}
                               for n, q in per.items()} for b, per in snap.items()}
        return snap


# --------------------------------------------------------------------------
# Strategy
# --------------------------------------------------------------------------
@dataclass
class Cand:
    strat: str
    base: str
    long_ex: str
    short_ex: str
    net_bps: float
    price_bps: float
    apr: float
    z: float = 0.0


def find_candidates(cfg, snap, basis=None, marks=None, relax=1.0):
    """All trade ideas in the snapshot. With a Basis (the default strategy) an idea is a coin whose gap between two exchanges is
    stretched far beyond its own normal level; we go long the cheap side and short the rich side and wait for it to snap back."""
    pa, fa = cfg["price_arb"], cfg["funding_arb"]
    out = []
    for base, per in snap.items():
        for x, qx in per.items():
            for y, qy in per.items():
                if x == y:
                    continue
                fees = 2 * (cfg["exchanges"][x]["taker_bps"] + cfg["exchanges"][y]["taker_bps"])
                pe = (qy["bid"] - qx["ask"]) / qx["ask"] * 1e4  # gap we capture if it converges
                if abs(pe) > cfg["max_price_gap_bps"]:
                    continue  # almost certainly a scale/contract-size mismatch, not real
                # Closing later means crossing the spread on BOTH exchanges again; the entry gap `pe` only paid it once.
                sx = (qx["ask"] - qx["bid"]) / qx["ask"] * 1e4
                sy = (qy["ask"] - qy["bid"]) / qy["ask"] * 1e4
                if max(sx, sy) > pa.get("max_spread_bps", 1e9):
                    continue  # thin book: the quoted gap is mostly spread, not an opportunity
                slip = cfg["slippage_buffer_bps"] + (sx + sy) / 2   # exit half-spread on each exchange
                if pa["enabled"]:
                    if basis is not None and pa.get("use_basis", True):
                        mean, sig, n = basis.stat(base, x, y)
                        if n >= pa.get("warmup_samples", 60):
                            g_now = gap_bps(qx, qy)
                            dev = g_now - mean
                            if marks and base in marks and x in marks[base] and y in marks[base]:
                                dev = min(dev, gap_bps(marks[base][x], marks[base][y]) - mean)   # book AND mark price must agree
                            sig = max(sig, pa.get("min_sigma_bps", 0.7))
                            z = dev / sig
                            if dev > 0 and z >= pa.get("z_min", 3.0) * relax:
                                # gap = (Y - X) / X; we are long X, short Y, so we win when the gap shrinks back to normal
                                out.append(Cand("price", base, x, y, dev - (sx + sy) - fees - cfg["slippage_buffer_bps"], dev, 0.0, z))
                    else:
                        out.append(Cand("price", base, x, y, pe - fees - slip, pe, 0.0))
                fx, fy = qx.get("funding_h"), qy.get("funding_h")
                if fa["enabled"] and fx is not None and fy is not None:
                    d = fy - fx  # short(Y) receives fy, long(X) pays fx
                    out.append(
                        Cand("funding", base, x, y, d * fa["expected_hold_hours"] * 1e4 + pe - fees - slip,
                             pe, d * 8760 * 100)
                    )
    return out


def passes(cfg, c):
    if c.strat == "price":
        return c.net_bps >= cfg["price_arb"]["min_net_edge_bps"]
    fa = cfg["funding_arb"]
    return c.apr >= fa["min_apr_pct"] and c.net_bps >= fa["min_net_edge_bps"]


@dataclass
class Position:
    id: str
    strat: str
    base: str
    long_ex: str
    short_ex: str
    notional: float
    qty_long: float
    qty_short: float
    entry_long: float
    entry_short: float
    opened: float
    last_ts: float
    funding_pnl: float = 0.0
    fees_paid: float = 0.0
    adv0: float = None   # price PnL (before fees) if closed right after entry = the spread we already paid; the stop-loss ignores it


def taker(cfg, ex):
    return cfg["exchanges"][ex]["taker_bps"] / 1e4


def valuation(cfg, pos, ql, qs, slip_bps):
    """Net PnL if we closed right now (price + funding - all fees)."""
    s = slip_bps / 1e4
    px_l = ql["bid"] * (1 - s)   # sell the long
    px_s = qs["ask"] * (1 + s)   # buy back the short
    gross = pos.qty_long * (px_l - pos.entry_long) + pos.qty_short * (pos.entry_short - px_s)
    close_fees = pos.qty_long * px_l * taker(cfg, pos.long_ex) + pos.qty_short * px_s * taker(cfg, pos.short_ex)
    return gross + pos.funding_pnl - pos.fees_paid - close_fees, px_l, px_s, close_fees


# --------------------------------------------------------------------------
# Brokers
# --------------------------------------------------------------------------
class PaperBroker:
    live = False

    def __init__(self, cfg):
        self.cfg = cfg

    def open(self, c, notional, snap):
        s = self.cfg["paper_extra_slippage_bps"] / 1e4
        px_l = snap[c.base][c.long_ex]["ask"] * (1 + s)
        px_s = snap[c.base][c.short_ex]["bid"] * (1 - s)
        ql, qs = notional / px_l, notional / px_s
        fees = notional * taker(self.cfg, c.long_ex) + notional * taker(self.cfg, c.short_ex)
        now = time.time()
        return Position(f"{c.base}-{c.strat}-{int(now)}", c.strat, c.base, c.long_ex, c.short_ex,
                        notional, ql, qs, px_l, px_s, now, now, 0.0, fees)

    def close(self, pos, snap):
        ql, qs = snap[pos.base][pos.long_ex], snap[pos.base][pos.short_ex]
        net, _, _, _ = valuation(self.cfg, pos, ql, qs, self.cfg["paper_extra_slippage_bps"])
        return net


class LiveBroker:
    """Real orders through ccxt. API keys come from environment variables:
    BINANCE_KEY/BINANCE_SECRET, BYBIT_KEY/BYBIT_SECRET, OKX_KEY/OKX_SECRET/OKX_PASSWORD,
    HYPERLIQUID_WALLET/HYPERLIQUID_PRIVATE_KEY.  Never enable withdrawals on these keys."""

    live = True

    def __init__(self, cfg, feed):
        import ccxt

        self.cfg, self.feed, self.ex = cfg, feed, {}
        for name in feed.ex:
            if name == "hyperliquid":
                creds = {"walletAddress": os.getenv("HYPERLIQUID_WALLET"),
                         "privateKey": os.getenv("HYPERLIQUID_PRIVATE_KEY")}
            else:
                u = name.upper()
                creds = {"apiKey": os.getenv(f"{u}_KEY"), "secret": os.getenv(f"{u}_SECRET")}
                if os.getenv(f"{u}_PASSWORD"):
                    creds["password"] = os.getenv(f"{u}_PASSWORD")
            if not all(creds.values()):
                log.warning("no credentials for %s -> excluded from live trading", name)
                continue
            ex = getattr(ccxt, name)({**creds, "enableRateLimit": True})
            ex.load_markets()
            self.ex[name] = ex
        if len(self.ex) < 2:
            raise SystemExit("Live mode needs credentials for at least 2 exchanges.")
        cfg["enabled_exchanges"] = list(self.ex)

    def _prep(self, name, base):
        ex, sym = self.ex[name], self.feed.sym[(name, base)]
        for fn, args in ((ex.set_margin_mode, ("isolated", sym)),
                         (ex.set_leverage, (self.cfg["sizing"]["leverage"], sym))):
            try:
                fn(*args)
            except Exception as e:
                log.debug("prep %s %s: %s", name, base, e)

    def _order(self, name, base, side, qty_base, ref_px, reduce):
        # qty_base and ref_px are per 1 coin; the exchange may count in 1000-coin contracts (1000PEPE / kPEPE)
        ex, sym = self.ex[name], self.feed.sym[(name, base)]
        cs, mult = self.feed.cs[(name, base)], self.feed.mult[(name, base)]
        amt = float(ex.amount_to_precision(sym, qty_base / (cs * mult)))
        o = ex.create_order(sym, "market", side, amt, ref_px * mult, {"reduceOnly": True} if reduce else {})
        avg = float(o.get("average") or o.get("price") or ref_px * mult) / mult
        return avg, float(o.get("filled") or amt) * cs * mult

    def _pair(self, legs):
        """Run both legs at the same time. Returns list of (result, error)."""
        with ThreadPoolExecutor(max_workers=2) as pool:
            futs = [pool.submit(self._order, *leg) for leg in legs]
        res = []
        for f in futs:
            try:
                res.append((f.result(), None))
            except Exception as e:
                res.append((None, e))
        return res

    def open(self, c, notional, snap):
        ql, qs = snap[c.base][c.long_ex], snap[c.base][c.short_ex]
        for n in (c.long_ex, c.short_ex):
            self._prep(n, c.base)
        q_l, q_s = notional / ql["ask"], notional / qs["bid"]
        (rl, el), (rs, es) = self._pair([
            (c.long_ex, c.base, "buy", q_l, ql["ask"], False),
            (c.short_ex, c.base, "sell", q_s, qs["bid"], False),
        ])
        if el or es:  # one leg failed: flatten the other immediately
            log.error("open leg failed long=%s short=%s -> unwinding", el, es)
            if rl:
                self._order(c.long_ex, c.base, "sell", rl[1], ql["bid"], True)
            if rs:
                self._order(c.short_ex, c.base, "buy", rs[1], qs["ask"], True)
            notify(f"ARB: open failed for {c.base}, legs unwound. long_err={el} short_err={es}")
            return None
        fees = rl[0] * rl[1] * taker(self.cfg, c.long_ex) + rs[0] * rs[1] * taker(self.cfg, c.short_ex)
        now = time.time()
        return Position(f"{c.base}-{c.strat}-{int(now)}", c.strat, c.base, c.long_ex, c.short_ex,
                        notional, rl[1], rs[1], rl[0], rs[0], now, now, 0.0, fees)

    def close(self, pos, snap):
        ql, qs = snap[pos.base][pos.long_ex], snap[pos.base][pos.short_ex]
        (rl, el), (rs, es) = self._pair([
            (pos.long_ex, pos.base, "sell", pos.qty_long, ql["bid"], True),
            (pos.short_ex, pos.base, "buy", pos.qty_short, qs["ask"], True),
        ])
        if el or es:
            log.error("CLOSE FAILED long=%s short=%s -- CHECK POSITIONS MANUALLY", el, es)
            notify(f"ARB: CLOSE FAILED {pos.id}. Check exchanges manually! long_err={el} short_err={es}")
            return None
        gross = pos.qty_long * (rl[0] - pos.entry_long) + pos.qty_short * (pos.entry_short - rs[0])
        fees = rl[0] * rl[1] * taker(self.cfg, pos.long_ex) + rs[0] * rs[1] * taker(self.cfg, pos.short_ex)
        return gross + pos.funding_pnl - pos.fees_paid - fees


# --------------------------------------------------------------------------
# Recording and replay: every scan is saved, then any settings can be tested on the SAME real market data in minutes
# --------------------------------------------------------------------------
class Recorder:
    """Appends one line per scan to <dir>/YYYY-MM-DD.jsonl.gz: mark price of every tracked coin on every exchange, plus
    bid/ask for the coins whose order book was fetched. Never raises: recording must not interfere with trading."""

    def __init__(self, cfg):
        rc = cfg.get("record") or {}
        self.on = bool(rc.get("enabled"))
        self.dir, self.keep_days = rc.get("dir", "recordings"), int(rc.get("keep_days", 14))
        self.last_clean = 0.0
        if self.on:
            try:
                os.makedirs(self.dir, exist_ok=True)
            except Exception as e:
                log.warning("recording disabled (cannot create %s: %s)", self.dir, e)
                self.on = False

    def write(self, now, marks, snap):
        if not self.on or not marks:
            return
        try:
            line = {"t": round(now, 1),
                    "m": {b: {n: round(q["bid"], 8) for n, q in per.items()} for b, per in marks.items()},
                    "f": {b: {n: q.get("funding_h") for n, q in per.items() if q.get("funding_h") is not None} for b, per in marks.items()},
                    "b": {b: {n: [round(q["bid"], 8), round(q["ask"], 8)] for n, q in per.items()} for b, per in snap.items()}}
            path = os.path.join(self.dir, time.strftime("%Y-%m-%d", time.gmtime(now)) + ".jsonl.gz")
            with gzip.open(path, "at", encoding="utf-8") as fh:
                fh.write(json.dumps(line, separators=(",", ":")) + "\n")
            if now - self.last_clean > 3600:
                self.last_clean = now
                for f in glob.glob(os.path.join(self.dir, "*.jsonl.gz")):
                    if now - os.path.getmtime(f) > self.keep_days * 86400:
                        os.remove(f)
        except Exception as e:
            log.warning("recording failed: %s", e)


def load_recording(path_or_dir):
    files = sorted(glob.glob(os.path.join(path_or_dir, "*.jsonl.gz"))) if os.path.isdir(path_or_dir) else [path_or_dir]
    frames = []
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for ln in fh:
                try:
                    frames.append(json.loads(ln))
                except ValueError:
                    pass
    frames.sort(key=lambda fr: fr["t"])
    return frames


class ReplayFeed:
    """Plays recorded scans back to the Bot, one per tick. For coins whose real order book was recorded we use its bid/ask; for the
    rest we assume `assumed_spread_bps` around the mark (an ASSUMPTION: replay is only as good as the book data recorded)."""

    def __init__(self, frames, assumed_spread_bps=1.0):
        self.frames, self.i, self.half = frames, -1, assumed_spread_bps / 2 / 1e4
        self.basis, self.last_rough, self.universe, self.positions = None, {}, 0, []
        self.t = 0.0

    def snapshot(self, bases, notional, depth_mult, keep=()):
        self.i += 1
        fr = self.frames[self.i]
        self.t = fr["t"]
        rough = {}
        for b, per in fr["m"].items():
            if len(per) < 2:
                continue
            rough[b] = {n: {"bid": px, "ask": px, "funding_h": (fr.get("f", {}).get(b, {}) or {}).get(n)} for n, px in per.items()}
        self.last_rough = rough
        snap = {}
        for b, per in rough.items():
            for n, q in per.items():
                bk = (fr.get("b", {}).get(b, {}) or {}).get(n)
                bid, ask = (bk if bk else (q["bid"] * (1 - self.half), q["ask"] * (1 + self.half)))
                snap.setdefault(b, {})[n] = {"bid": bid, "ask": ask, "funding_h": q["funding_h"]}
        return snap


class _Clock:
    """Stand-in for the `time` module during replay so the bot lives in the recording's time, not the real one."""
    now = 0.0

    def time(self):
        return self.now

    def gmtime(self, secs=None):
        return time_real.gmtime(self.now if secs is None else secs)

    def localtime(self, secs=None):
        return time_real.localtime(self.now if secs is None else secs)

    def strftime(self, fmt, tup=None):
        return time_real.strftime(fmt, tup if tup is not None else time_real.gmtime(self.now))

    def sleep(self, _):
        pass

    def __getattr__(self, k):
        return getattr(time_real, k)


time_real = time


def run_replay(base_cfg, frames, overrides=None, assumed_spread_bps=1.0):
    """Run the bot over recorded frames with optional setting overrides. Returns a result dict. Runs in a temp folder."""
    global time, notify
    import copy
    cfg = copy.deepcopy(base_cfg)
    for k, v in (overrides or {}).items():
        sect, _, key = k.partition(".")
        if key:
            cfg[sect][key] = v
        else:
            cfg[sect] = v
    cfg["schedule"]["pause_weekends"] = bool(cfg["schedule"].get("pause_weekends"))
    cfg["record"] = {"enabled": False}
    cfg["bases"] = sorted({b for fr in frames for b in fr["m"]})
    tmp = tempfile.mkdtemp(prefix="replay_")
    cfg["files"] = {"state": os.path.join(tmp, "state.json"), "trades": os.path.join(tmp, "trades.csv")}
    clock, saved_notify = _Clock(), notify
    feed = ReplayFeed(frames, assumed_spread_bps)
    time, notify = clock, (lambda m: None)
    try:
        clock.now = frames[0]["t"]
        bot = Bot(cfg, feed, PaperBroker(cfg))
        bot.started = clock.now
        for fr in frames:
            clock.now = fr["t"]
            bot.tick()
        for pos in list(bot.positions):   # close what is still open at the end, at the last prices
            bot.close_pos(pos, bot.last_snap, "end_of_replay")
    finally:
        time, notify = time_real, saved_notify
    rows = read_closes(cfg["files"]["trades"])
    pnls = [r["pnl"] for r in rows]
    per_coin = {}
    for r in rows:
        per_coin[r["base"]] = per_coin.get(r["base"], 0.0) + r["pnl"]
    reasons = {}
    for r in rows:
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
    return {"trades": len(rows), "pnl": round(sum(pnls), 3), "wins": sum(1 for x in pnls if x > 0),
            "avg": round(sum(pnls) / len(pnls), 4) if pnls else 0.0, "per_coin": per_coin, "reasons": reasons,
            "hours": round((frames[-1]["t"] - frames[0]["t"]) / 3600, 1)}


def replay_cli(cfg, path, grid_args, spread_bps):
    import itertools
    frames = load_recording(path)
    if len(frames) < 10:
        raise SystemExit(f"Not enough recorded scans in {path} ({len(frames)}). Let the bot run and record for a day or more first.")
    keys, vals = [], []
    for g in grid_args or []:
        k, _, v = g.partition("=")
        keys.append(k)
        vals.append([float(x) if re.match(r"^-?\d+(\.\d+)?$", x) else x for x in v.split(",")])
    combos = list(itertools.product(*vals)) if keys else [()]
    logging.getLogger("arb").setLevel(logging.WARNING)
    print(f"Replaying {len(frames)} recorded scans ({(frames[-1]['t'] - frames[0]['t']) / 3600:.1f} hours, "
          f"{len({b for fr in frames for b in fr['m']})} coins). Spread assumed for coins without a recorded book: {spread_bps} bps.")
    print(f"{'settings':52s} {'trades':>6s} {'win%':>5s} {'avg$':>8s} {'total$':>8s}")
    best = None
    for combo in combos:
        ov = dict(zip(keys, combo))
        r = run_replay(cfg, frames, ov, spread_bps)
        label = ", ".join(f"{k}={v:g}" if isinstance(v, float) else f"{k}={v}" for k, v in ov.items()) or "current config"
        wr = (100 * r["wins"] / r["trades"]) if r["trades"] else 0
        print(f"{label:52s} {r['trades']:6d} {wr:5.0f} {r['avg']:8.3f} {r['pnl']:8.2f}")
        if best is None or r["pnl"] > best[1]["pnl"]:
            best = (label, r)
    if best and best[1]["trades"]:
        r = best[1]
        print(f"\nBest: {best[0]}  ({r['trades']} trades, total {r['pnl']:+.2f} USD)")
        print("Exit reasons:", r["reasons"])
        print("By coin:", {k: round(v, 2) for k, v in sorted(r["per_coin"].items(), key=lambda kv: kv[1])})
    print("\nCaution: the best row of a sweep is partly luck. Only trust a setting that also works on data it was NOT tuned on.")


# --------------------------------------------------------------------------
# Bot
# --------------------------------------------------------------------------
class Bot:
    def __init__(self, cfg, feed, broker):
        self.cfg, self.feed, self.broker = cfg, feed, broker
        self.running = True
        self.sf, self.tf = cfg["files"]["state"], cfg["files"]["trades"]
        self.st = {"positions": [], "day": "", "day_pnl": 0.0, "total_pnl": 0.0, "cooldowns": {}}
        if os.path.exists(self.sf):
            with open(self.sf) as f:
                self.st.update(json.load(f))
        self.positions = [Position(**p) for p in self.st["positions"]]
        self.killed = False
        self.seen = {}
        self.basis = Basis(cfg["price_arb"].get("ewma_alpha", 0.005), self.st.get("basis"), cfg["max_price_gap_bps"])
        self.feed.basis = self.basis
        self.weekend = False
        self.rec = Recorder(cfg)
        self.alerted_stale = False
        self.paused = bool(self.st.get("paused"))   # set by /pause, survives restarts
        self.close_all = False                       # set by /closeall
        self.last_snap, self.last_tick = {}, 0.0
        self.started = time.time()

    def save(self):
        self.st["paused"] = self.paused
        self.st["basis"] = self.basis.s
        self.st["positions"] = [asdict(p) for p in self.positions]
        tmp = self.sf + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.st, f, indent=1)
        os.replace(tmp, self.sf)

    def record(self, event, pos, pnl="", reason=""):
        new = not os.path.exists(self.tf)
        with open(self.tf, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time_utc", "event", "strategy", "base", "long_ex", "short_ex",
                            "notional", "pnl_usd", "reason"])
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), event, pos.strat, pos.base,
                        pos.long_ex, pos.short_ex, round(pos.notional, 2), pnl, reason])

    def exit_reason(self, pos, net, ql, qs, now, adverse):
        cfg = self.cfg
        n = pos.notional
        age_h = (now - pos.opened) / 3600
        if self.killed:
            return "kill_switch"
        if self.close_all:
            return "manual_close"
        if pos.strat == "price":
            p = cfg["price_arb"]
            if adverse <= -p["stop_loss_bps"] / 1e4 * n:   # price/funding loss, fees excluded
                return "stop_loss"
            if net >= p["take_profit_bps"] / 1e4 * n:
                return "take_profit"
            if age_h * 60 >= p["max_hold_minutes"]:
                return "timeout"
        else:
            p = cfg["funding_arb"]
            if adverse <= -p["stop_loss_bps"] / 1e4 * n:   # price/funding loss, fees excluded
                return "stop_loss"
            if ql.get("funding_h") is not None and qs.get("funding_h") is not None and age_h >= p.get("min_hold_hours", 0):
                if (qs["funding_h"] - ql["funding_h"]) * 8760 * 100 <= p["exit_apr_pct"]:
                    return "funding_faded"
            if age_h >= p["max_hold_hours"]:
                return "timeout"
        return None

    def close_pos(self, pos, snap, reason):
        pnl = self.broker.close(pos, snap)
        if pnl is None:
            return
        self.positions.remove(pos)
        self.st["day_pnl"] += pnl
        self.st["total_pnl"] += pnl
        self.st["cooldowns"][pos.base] = time.time()
        rk = self.cfg["risk"]
        streak = self.st.setdefault("loss_streak", {})
        streak[pos.base] = (streak.get(pos.base, 0) + 1) if pnl < 0 else 0
        if streak[pos.base] >= int(rk.get("ban_after_losses", 0) or 0) > 0:
            self.st.setdefault("bans", {})[pos.base] = time.time() + float(rk.get("ban_hours", 24)) * 3600
            streak[pos.base] = 0
            log.info("coin %s banned for %sh after repeated losses", pos.base, rk.get("ban_hours", 24))
        self.record("CLOSE", pos, round(pnl, 4), reason)
        msg = f"[{'LIVE' if self.broker.live else 'PAPER'}] CLOSE {pos.strat} {pos.base} " \
              f"L:{pos.long_ex} S:{pos.short_ex} pnl={pnl:+.3f} USD ({reason}) day={self.st['day_pnl']:+.2f}"
        log.info(msg)
        notify(msg)

    def tick(self):
        cfg, sz = self.cfg, self.cfg["sizing"]
        now = time.time()
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        if day != self.st["day"]:
            if self.st["day"]:
                try:
                    notify(f"Daily report {self.st['day']} (UTC): {self.st['day_pnl']:+.2f} USD | all-time {self.st['total_pnl']:+.2f} USD | "
                           f"open positions {len(self.positions)}")
                except Exception:
                    pass
            self.st["day"], self.st["day_pnl"], self.killed = day, 0.0, False

        snap = self.feed.snapshot(cfg["bases"], sz["notional_usd"], sz["min_depth_multiple"],
                                  keep=[p.base for p in self.positions])
        if not snap:
            log.warning("no market data this cycle")
            return
        self.last_snap, self.last_tick = snap, now
        self.rec.write(now, getattr(self.feed, "last_rough", None), snap)
        if self.alerted_stale:
            self.alerted_stale = False
            notify("Scanning is working again.")

        if not self.killed and self.st["day_pnl"] <= -cfg["risk"]["daily_loss_limit_usd"]:
            self.killed = True
            msg = f"KILL SWITCH: daily loss {self.st['day_pnl']:.2f} hit limit. Closing all, no new trades today."
            log.warning(msg)
            notify(msg)

        closing_all = self.close_all  # no new entries on the scan that executes /closeall

        # 1) manage open positions
        for pos in list(self.positions):
            per = snap.get(pos.base, {})
            ql, qs = per.get(pos.long_ex), per.get(pos.short_ex)
            if not ql or not qs:
                log.warning("no data for open position %s this cycle", pos.id)
                continue
            if ql.get("funding_h") is not None and qs.get("funding_h") is not None:
                dt_h = (now - pos.last_ts) / 3600
                pos.funding_pnl += pos.notional * (qs["funding_h"] - ql["funding_h"]) * dt_h
            pos.last_ts = now
            net, _, _, close_fees = valuation(cfg, pos, ql, qs, cfg["paper_extra_slippage_bps"])
            adverse = net + pos.fees_paid + close_fees   # PnL before fees: what the stop-loss watches
            if pos.adv0 is None:
                pos.adv0 = adverse
            adverse -= pos.adv0                          # only count movement AFTER entry, not the spread paid to get in
            reason = self.exit_reason(pos, net, ql, qs, now, adverse)
            if reason:
                self.close_pos(pos, snap, reason)
        if self.close_all and not self.positions:
            self.close_all = False

        # 2) look for new entries
        marks = getattr(self.feed, "last_rough", None) or None
        cands = find_candidates(cfg, snap, self.basis, marks)
        self.basis.update(marks or snap)      # learn from this scan AFTER judging it
        weekend = weekend_active(cfg, now)
        if weekend != self.weekend:
            self.weekend = weekend
            msg = ("WEEKEND PAUSE: no new trades until Monday (open positions are still managed)." if weekend
                   else "Weekend over: trading resumes.")
            log.info(msg)
            notify(msg)
        for strat in ("price", "funding"):
            best = max((c for c in cands if c.strat == strat), key=lambda c: c.net_bps, default=None)
            if best:
                log.info("best %-7s %s long %s / short %s  net %.1f bps%s", strat, best.base, best.long_ex,
                         best.short_ex, best.net_bps, f"  apr {best.apr:.0f}%" if strat == "funding" else f"  z {best.z:.1f}")

        # an opportunity must survive several scans in a row (one-scan blips are mostly stale quotes)
        keys = {(c.strat, c.base, c.long_ex, c.short_ex) for c in cands if passes(cfg, c)}
        self.seen = {k: self.seen.get(k, 0) + 1 for k in keys}
        need_p = int(cfg["price_arb"].get("confirm_scans", 1))
        need_f = int(cfg["funding_arb"].get("confirm_scans", 1))
        if not self.killed and not self.paused and not closing_all and not weekend:
            for c in sorted((c for c in cands if passes(cfg, c)), key=lambda c: -c.net_bps):
                if self.seen.get((c.strat, c.base, c.long_ex, c.short_ex), 0) < (need_p if c.strat == "price" else need_f):
                    continue
                if self.st.get("bans", {}).get(c.base, 0) > now:
                    continue
                if len(self.positions) >= sz["max_open_positions"]:
                    break
                if sum(p.notional for p in self.positions) + sz["notional_usd"] > sz["max_total_notional_usd"]:
                    break
                if any(p.base == c.base for p in self.positions):
                    continue
                if now - self.st["cooldowns"].get(c.base, 0) < cfg["risk"]["cooldown_minutes"] * 60:
                    continue
                pos = self.broker.open(c, sz["notional_usd"], snap)
                if pos:
                    try:
                        ql0, qs0 = snap[c.base][c.long_ex], snap[c.base][c.short_ex]
                        n0, _, _, cf0 = valuation(cfg, pos, ql0, qs0, cfg["paper_extra_slippage_bps"])
                        pos.adv0 = n0 + pos.fees_paid + cf0
                    except Exception:
                        pos.adv0 = None
                    self.positions.append(pos)
                    self.record("OPEN", pos, "", f"net_edge={c.net_bps:.1f}bps z={c.z:.1f}")
                    msg = f"[{'LIVE' if self.broker.live else 'PAPER'}] OPEN {c.strat} {c.base} " \
                          f"L:{c.long_ex} S:{c.short_ex} ${sz['notional_usd']} edge={c.net_bps:.1f}bps"
                    log.info(msg)
                    notify(msg)
        self.save()

    def _watchdog(self):
        wd = self.cfg.get("watchdog") or {}
        while self.running:
            time.sleep(15)
            try:
                ref = self.last_tick or self.started
                if time.time() - ref > float(wd.get("stale_seconds", 120)) and not self.alerted_stale:
                    self.alerted_stale = True
                    notify(f"WARNING: no market scan for {int(time.time() - ref)}s. The exchange API may be down or slow. "
                           f"Open positions: {len(self.positions)}. Check Railway logs.")
            except Exception:
                pass

    def run(self, max_loops=None):
        if (self.cfg.get("watchdog") or {}).get("enabled") and max_loops is None:
            threading.Thread(target=self._watchdog, daemon=True).start()
        n = 0
        while self.running and (max_loops is None or n < max_loops):
            t0 = time.time()
            try:
                self.tick()
            except Exception:
                log.exception("tick failed (continuing)")
            n += 1
            time.sleep(max(0, self.cfg["loop_seconds"] - (time.time() - t0)))
        self.save()
        log.info("stopped. open positions kept in %s: %d | total pnl %+.2f USD",
                 self.sf, len(self.positions), self.st["total_pnl"])


# --------------------------------------------------------------------------
# Telegram commands (only the chat in TELEGRAM_CHAT_ID is obeyed)
# --------------------------------------------------------------------------
def fmt_dur(s):
    s = int(s)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    return (f"{d}d " if d else "") + (f"{h}h " if h or d else "") + f"{s // 60}m"


def read_closes(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("event") != "CLOSE":
                continue
            try:
                r["pnl"] = float(r["pnl_usd"])
                r["ts"] = calendar.timegm(time.strptime(r["time_utc"], "%Y-%m-%d %H:%M:%S"))
            except (ValueError, KeyError, TypeError):
                continue
            rows.append(r)
    return rows


def agg(rows):
    n = len(rows)
    total = sum(r["pnl"] for r in rows)
    wins = sum(1 for r in rows if r["pnl"] > 0)
    return f"{total:+.2f} USD ({n} trades, {wins / n * 100:.0f}% win)" if n else "0.00 USD (0 trades)"


class TelegramCommands(threading.Thread):
    COMMANDS = [
        ("status", "Bot state, uptime, open positions, PnL"),
        ("positions", "Open positions with live PnL"),
        ("pnl", "Profit/loss: today, 7 days, all time"),
        ("trades", "Last closed trades (/trades 10)"),
        ("coins", "Per-coin results and coins being scanned"),
        ("best", "Best opportunities right now vs. entry thresholds"),
        ("config", "Current strategy settings"),
        ("pause", "Stop opening NEW trades (open ones keep being managed)"),
        ("resume", "Allow new trades again"),
        ("closeall", "Close every open position (needs: /closeall confirm)"),
        ("help", "Show this list"),
    ]

    def __init__(self, bot):
        super().__init__(daemon=True)
        self.bot = bot
        self.tok = os.getenv("TELEGRAM_TOKEN")
        self.chat = str(os.getenv("TELEGRAM_CHAT_ID") or "")

    def api(self, method, **params):
        req = urllib.request.Request(f"https://api.telegram.org/bot{self.tok}/{method}",
                                     data=urllib.parse.urlencode(params).encode())
        with urllib.request.urlopen(req, timeout=40) as r:
            return json.load(r)

    def clear_webhook(self):
        """If any other program ever pointed this bot at a webhook (for example the ELPP bot server), Telegram sends our commands THERE
        and they get an "Access Denied" reply. Long-polling only works with no webhook, so remove it."""
        try:
            info = self.api("getWebhookInfo").get("result", {})
            if info.get("url"):
                log.warning("this Telegram bot had a webhook set (%s); removing it so commands reach this bot", info["url"].split("?")[0][:60])
            self.api("deleteWebhook", drop_pending_updates="true")
        except Exception as e:
            log.warning("could not clear the Telegram webhook: %s", e)

    def run(self):
        if not self.tok or not self.chat:
            log.info("Telegram commands off (set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID to enable)")
            return
        offset = None
        self.clear_webhook()
        try:
            self.api("setMyCommands", commands=json.dumps(
                [{"command": c, "description": d[:250]} for c, d in self.COMMANDS]))
            # discard anything sent while we were down, so an old /closeall can't fire on restart
            res = self.api("getUpdates", offset=-1, timeout=0).get("result", [])
            if res:
                offset = res[-1]["update_id"] + 1
        except Exception as e:
            log.warning("telegram init failed: %s", e)
        notify(f"Arb bot online ({'LIVE' if self.bot.broker.live else 'PAPER'}). Send /help for commands.")
        while self.bot.running:
            try:
                params = {"timeout": 25}
                if offset is not None:
                    params["offset"] = offset
                for u in self.api("getUpdates", **params).get("result", []):
                    offset = u["update_id"] + 1
                    msg = u.get("message") or {}
                    if str(msg.get("chat", {}).get("id")) != self.chat:
                        continue  # ignore everyone except the owner
                    text = (msg.get("text") or "").strip()
                    if text.startswith("/"):
                        parts = text.split()
                        cmd = parts[0].split("@")[0].lower()
                        try:
                            reply = self.handle(cmd, parts[1:])
                        except Exception as e:
                            log.exception("command %s failed", cmd)
                            reply = f"Command failed: {e}"
                        notify(reply[:4000])
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    log.warning("telegram 409: a webhook or another process is using this bot token; clearing the webhook. "
                                "If it keeps happening, another program is polling the same token: use a SEPARATE bot token.")
                    self.clear_webhook()
                    time.sleep(30)
                else:
                    log.warning("telegram http %s", e.code)
                    time.sleep(5)
            except Exception as e:
                log.debug("telegram poll: %s", e)
                time.sleep(5)

    # ---- command handlers ----
    def handle(self, cmd, args):
        b, cfg = self.bot, self.bot.cfg
        if cmd in ("/start", "/help"):
            return "Commands:\n" + "\n".join(f"/{c} - {d}" for c, d in self.COMMANDS)
        if cmd == "/status":
            ex = list(getattr(b.feed, "ex", None) or cfg["enabled_exchanges"])
            state = "PAUSED (no new trades)" if b.paused else ("KILL SWITCH (daily loss limit)" if b.killed else ("WEEKEND PAUSE (no new trades)" if b.weekend else "running"))
            wu = cfg["price_arb"].get("warmup_samples", 60)
            ready = sum(1 for c in cfg["bases"] if b.basis.ready(c, wu))
            scan = f"{int(time.time() - b.last_tick)}s ago" if b.last_tick else "not yet"
            return (f"{'LIVE' if b.broker.live else 'PAPER'} bot: {state}\n"
                    f"Uptime {fmt_dur(time.time() - b.started)} | last scan {scan}\n"
                    f"Exchanges: {', '.join(ex)}\n"
                    f"Coins tracked: {len(cfg['bases'])} liquid coins | learned normal gap: {ready}/{len(cfg['bases'])} ready\n"
                    f"Order-book checked last scan: {len(b.last_snap)}\n"
                    f"Open positions: {len(b.positions)}/{cfg['sizing']['max_open_positions']}\n"
                    f"Today (UTC): {b.st['day_pnl']:+.2f} USD | all-time: {b.st['total_pnl']:+.2f} USD")
        if cmd == "/positions":
            if not b.positions:
                return "No open positions."
            out, tot = [], 0.0
            for p in list(b.positions):
                per = b.last_snap.get(p.base, {})
                ql, qs = per.get(p.long_ex), per.get(p.short_ex)
                now_s = "n/a"
                if ql and qs:
                    net = valuation(cfg, p, ql, qs, cfg["paper_extra_slippage_bps"])[0]
                    tot += net
                    now_s = f"{net:+.3f} USD"
                out.append(f"{p.base} {p.strat} | L:{p.long_ex} S:{p.short_ex} | ${p.notional:.0f} | "
                           f"{fmt_dur(time.time() - p.opened)} | funding {p.funding_pnl:+.3f} | if closed now {now_s}")
            return f"Open positions ({len(out)}):\n" + "\n".join(out) + f"\nTotal if closed now: {tot:+.3f} USD"
        if cmd == "/pnl":
            rows = read_closes(b.tf)
            today = time.strftime("%Y-%m-%d", time.gmtime())
            wk = time.time() - 7 * 86400
            return (f"PnL (closed trades, UTC)\n"
                    f"Today: {agg([r for r in rows if r['time_utc'].startswith(today)])}\n"
                    f"Last 7 days: {agg([r for r in rows if r['ts'] >= wk])}\n"
                    f"All time: {agg(rows)}")
        if cmd == "/trades":
            n = 5
            if args and args[0].isdigit():
                n = max(1, min(20, int(args[0])))
            rows = read_closes(b.tf)[-n:]
            if not rows:
                return "No closed trades yet."
            return f"Last {len(rows)} closed trades:\n" + "\n".join(
                f"{r['time_utc'][5:16]} {r['base']} {r['strategy']} {r['pnl']:+.3f} USD ({r['reason']})"
                for r in reversed(rows))
        if cmd == "/coins":
            per_coin = {}
            for r in read_closes(b.tf):
                per_coin.setdefault(r["base"], []).append(r)
            lines = [f"{c}: {agg(rs)}" for c, rs in sorted(per_coin.items(), key=lambda kv: -sum(x['pnl'] for x in kv[1]))]
            return ("Per-coin results:\n" + ("\n".join(lines) if lines else "no closed trades yet") +
                    f"\n\nTracking {len(cfg['bases'])} coins:\n{' '.join(cfg['bases'])}")
        if cmd == "/best":
            if not b.last_snap:
                return "No market data yet."
            pa = cfg["price_arb"]
            cands = find_candidates(cfg, b.last_snap, b.basis, getattr(b.feed, "last_rough", None) or None, 0.0)
            top = sorted(cands, key=lambda c: -c.z)[:5]
            out = [f"Most stretched gaps now (entry needs z >= {pa['z_min']} and net >= {pa['min_net_edge_bps']} bps):"]
            for c in top:
                out.append(f"  {c.base} L:{c.long_ex} S:{c.short_ex} z {c.z:.1f}, stretch {c.price_bps:+.1f} bps, net {c.net_bps:+.1f} bps"
                           f"{'  <- ENTRY' if passes(cfg, c) and c.z >= pa['z_min'] else ''}")
            if not top:
                out.append("  none: every gap is within its normal range (or still warming up)")
            return "\n".join(out)
        if cmd == "/config":
            pa, sz = cfg["price_arb"], cfg["sizing"]
            sc = cfg.get("schedule") or {}
            return (f"Mode: {'LIVE' if b.broker.live else 'PAPER'}\n"
                    f"Size ${sz['notional_usd']}/leg, {sz['leverage']}x, max {sz['max_open_positions']} positions, "
                    f"max ${sz['max_total_notional_usd']} total\n"
                    f"Strategy: convergence trade. Enter when the gap is >= {pa['z_min']} std-devs from its normal level "
                    f"and net edge >= {pa['min_net_edge_bps']} bps; TP {pa['take_profit_bps']} / SL {pa['stop_loss_bps']} bps, "
                    f"max hold {pa['max_hold_minutes']}m\n"
                    f"Weekend pause: {'on (' + str(sc.get('timezone', 'UTC')) + ')' if sc.get('pause_weekends') else 'off'}\n"
                    f"Daily loss limit ${cfg['risk']['daily_loss_limit_usd']}, cooldown {cfg['risk']['cooldown_minutes']}m, "
                    f"coin ban after {cfg['risk'].get('ban_after_losses')} losses in a row\n"
                    f"Loop {cfg['loop_seconds']}s")
        if cmd == "/pause":
            b.paused = True
            return "Paused: no NEW trades. Open positions are still managed. /resume to continue."
        if cmd == "/resume":
            b.paused = False
            return "Resumed: new trades allowed."
        if cmd == "/closeall":
            if not b.positions:
                return "No open positions to close."
            if not args or args[0].lower() != "confirm":
                return f"This closes all {len(b.positions)} open position(s) at market. Send /closeall confirm to proceed."
            b.close_all = True
            return f"Closing {len(b.positions)} position(s) on the next scan (within ~{cfg['loop_seconds']}s). New trades blocked until done."
        return "Unknown command. Send /help."


# --------------------------------------------------------------------------
# Read-only website API (for the ELPP Data Desk "Arbitrage" section)
#   Off unless ARB_API_KEY is set. Every request needs header  X-API-Key: <ARB_API_KEY>.
#   Exposes trade data only: no exchange keys, no order placement, nothing writable.
# --------------------------------------------------------------------------
def _iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def load_trades(path):
    """Closed trades from trades.csv, each paired with its OPEN row (first-in-first-out per coin/strategy/exchange pair)."""
    pend, out = {}, []
    if not os.path.exists(path):
        return out
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            try:
                ts = calendar.timegm(time.strptime(r["time_utc"], "%Y-%m-%d %H:%M:%S"))
            except (ValueError, KeyError, TypeError):
                continue
            key = (r.get("strategy"), r.get("base"), r.get("long_ex"), r.get("short_ex"))
            if r.get("event") == "OPEN":
                pend.setdefault(key, []).append(ts)
            elif r.get("event") == "CLOSE":
                try:
                    pnl, notional = float(r["pnl_usd"]), float(r["notional"])
                except (ValueError, KeyError, TypeError):
                    continue
                opened = pend[key].pop(0) if pend.get(key) else None
                out.append({"closed": _iso(ts), "opened": _iso(opened) if opened else None,
                            "held_s": (ts - opened) if opened else None, "coin": r.get("base", ""),
                            "strategy": r.get("strategy", ""), "long_ex": r.get("long_ex", ""),
                            "short_ex": r.get("short_ex", ""), "notional": notional, "pnl": round(pnl, 4),
                            "pnl_bps": round(pnl / notional * 1e4, 1) if notional else 0.0,
                            "reason": r.get("reason", "")})
    return out


def build_overview(bot):
    cfg, trades = bot.cfg, load_trades(bot.tf)
    pnls = [t["pnl"] for t in trades]
    wins, losses = [p for p in pnls if p > 0], [p for p in pnls if p < 0]
    held = [t["held_s"] for t in trades if t["held_s"] is not None]
    coins = {}
    for t in trades:
        c = coins.setdefault(t["coin"], {"coin": t["coin"], "trades": 0, "wins": 0, "pnl": 0.0})
        c["trades"] += 1
        c["wins"] += 1 if t["pnl"] > 0 else 0
        c["pnl"] = round(c["pnl"] + t["pnl"], 4)
    positions, unreal = [], 0.0
    for p in list(bot.positions):
        per = bot.last_snap.get(p.base, {})
        ql, qs = per.get(p.long_ex), per.get(p.short_ex)
        now_pnl = None
        if ql and qs:
            now_pnl = round(valuation(cfg, p, ql, qs, cfg["paper_extra_slippage_bps"])[0], 4)
            unreal += now_pnl
        positions.append({"coin": p.base, "strategy": p.strat, "long_ex": p.long_ex, "short_ex": p.short_ex,
                          "notional": p.notional, "opened": _iso(p.opened), "age_s": int(time.time() - p.opened),
                          "funding_pnl": round(p.funding_pnl, 4), "pnl_if_closed_now": now_pnl})
    state = "kill_switch" if bot.killed else ("paused" if (bot.paused or bot.weekend) else "running")
    return {
        "mode": "LIVE" if bot.broker.live else "PAPER", "state": state,
        "uptime_s": int(time.time() - bot.started),
        "last_scan_s_ago": int(time.time() - bot.last_tick) if bot.last_tick else None,
        "exchanges": list(getattr(bot.feed, "ex", None) or cfg["enabled_exchanges"]),
        "coins_tracked": len(cfg["bases"]) if isinstance(cfg["bases"], list) else 0,
        "pnl": {"total": round(sum(pnls), 4), "unrealised": round(unreal, 4)},
        "stats": {"trades": len(pnls), "wins": len(wins), "losses": len(losses),
                  "win_rate": round(len(wins) / len(pnls) * 100, 1) if pnls else None,
                  "avg": round(sum(pnls) / len(pnls), 4) if pnls else None,
                  "best": max(pnls) if pnls else None, "worst": min(pnls) if pnls else None,
                  "profit_factor": round(sum(wins) / -sum(losses), 2) if losses else None,
                  "avg_hold_s": int(sum(held) / len(held)) if held else None},
        "positions": positions,
        "coins": sorted(coins.values(), key=lambda c: -c["pnl"]),
        "closed": [{"t": t["closed"], "pnl": t["pnl"]} for t in trades[-5000:]],   # the page draws the curve / today / 7-day from this
        "settings": {"notional_usd": cfg["sizing"]["notional_usd"], "leverage": cfg["sizing"]["leverage"],
                     "max_open_positions": cfg["sizing"]["max_open_positions"]},
    }


def build_trades(bot, q):
    rows = load_trades(bot.tf)
    strat, coin, res = q.get("strategy", [""])[0], q.get("coin", [""])[0].upper(), q.get("result", [""])[0]
    if strat in ("price", "funding"):
        rows = [t for t in rows if t["strategy"] == strat]
    if coin:
        rows = [t for t in rows if coin in t["coin"].upper()]
    if res == "win":
        rows = [t for t in rows if t["pnl"] > 0]
    elif res == "loss":
        rows = [t for t in rows if t["pnl"] < 0]
    rows.reverse()  # newest first
    try:
        limit, offset = max(1, min(500, int(q.get("limit", ["50"])[0]))), max(0, int(q.get("offset", ["0"])[0]))
    except ValueError:
        limit, offset = 50, 0
    return {"total": len(rows), "offset": offset, "rows": rows[offset:offset + limit]}


class _DualStackServer(ThreadingHTTPServer):
    """Listens on IPv6 AND IPv4 (Railway's private network is IPv6)."""
    address_family = socket.AF_INET6
    daemon_threads = True

    def server_bind(self):
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except Exception:
            pass
        super().server_bind()


def make_api_handler(bot, key):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # keep Railway logs clean
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            if u.path in ("/", "/health"):
                return self._send(200, {"ok": True, "service": "arb-bot"})
            given = (self.headers.get("X-API-Key") or "").strip()
            if not hmac.compare_digest(given, key):
                return self._send(401, {"ok": False, "error": "unauthorized"})
            try:
                if u.path == "/api/arb/overview":
                    return self._send(200, {"ok": True, "data": build_overview(bot)})
                if u.path == "/api/arb/trades":
                    return self._send(200, {"ok": True, "data": build_trades(bot, urllib.parse.parse_qs(u.query))})
                return self._send(404, {"ok": False, "error": "not found"})
            except Exception as e:
                log.exception("api error")
                return self._send(500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
    return H


class ArbApi(threading.Thread):
    def __init__(self, bot):
        super().__init__(daemon=True)
        self.bot = bot

    def run(self):
        key = (os.getenv("ARB_API_KEY") or "").strip()
        if not key:
            log.info("website API off (set ARB_API_KEY to show trades in the ELPP Data Desk)")
            return
        port = int(os.getenv("ARB_API_PORT") or os.getenv("PORT") or 8080)
        handler = make_api_handler(self.bot, key)
        try:
            srv = _DualStackServer(("::", port), handler)
        except OSError:  # no IPv6 on this host
            srv = ThreadingHTTPServer(("0.0.0.0", port), handler)
        log.info("website API listening on port %d (read-only, key protected)", port)
        srv.serve_forever()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--demo", action="store_true", help="synthetic data, offline logic test")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--yes-real-money", action="store_true")
    ap.add_argument("--max-loops", type=int)
    ap.add_argument("--replay", metavar="PATH", help="replay recorded scans (a folder or one .jsonl.gz file) instead of trading")
    ap.add_argument("--grid", action="append", metavar="KEY=V1,V2", help="with --replay: try settings, e.g. --grid price_arb.z_min=2.5,3,3.5")
    ap.add_argument("--assumed-spread-bps", type=float, default=1.0, help="with --replay: spread assumed where no order book was recorded")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config(a.config)

    if a.replay:
        return replay_cli(cfg, a.replay, a.grid, a.assumed_spread_bps)

    if a.demo:
        cfg["price_arb"]["warmup_samples"] = 30           # demo only: learn quickly
        cfg["schedule"]["pause_weekends"] = False         # demo only: trade even if today is a weekend
        if cfg["bases"] in ("auto", "liquid"):
            cfg["bases"] = ["BTC", "ETH", "SOL", "HYPE", "DOGE", "SUI", "AVAX", "LINK", "ARB", "WIF",
                            "TIA", "INJ", "NEAR", "APT", "ENA", "PEPE", "BONK", "FARTCOIN"]
        cfg["loop_seconds"] = 0.05
        cfg["files"] = {"state": "demo_state.json", "trades": "demo_trades.csv"}
        feed, broker = MockFeed(cfg), PaperBroker(cfg)
        log.info("DEMO mode: synthetic data, no network, no orders")
    else:
        live = a.live and a.yes_real_money and cfg["mode"] == "live"
        if (a.live or cfg["mode"] == "live") and not live:
            raise SystemExit("Live needs ALL of: mode: live in config, --live, --yes-real-money.")
        feed = Feed(cfg)
        broker = LiveBroker(cfg, feed) if live else PaperBroker(cfg)
        log.warning("*** LIVE TRADING WITH REAL MONEY ***") if live else log.info("PAPER mode (no real orders)")

    bot = Bot(cfg, feed, broker)
    if not a.demo:
        TelegramCommands(bot).start()
    ArbApi(bot).start()
    signal.signal(signal.SIGINT, lambda *_: setattr(bot, "running", False))
    signal.signal(signal.SIGTERM, lambda *_: setattr(bot, "running", False))
    bot.run(a.max_loops)


if __name__ == "__main__":
    main()
