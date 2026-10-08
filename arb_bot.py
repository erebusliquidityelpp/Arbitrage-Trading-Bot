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
import copy
import csv
import json
import logging
import os
import random
import signal
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass

import yaml

log = logging.getLogger("arb")

DEFAULTS = {
    "mode": "paper",
    "loop_seconds": 10,
    "bases": ["BTC", "ETH", "SOL"],
    "enabled_exchanges": ["binance", "bybit", "okx", "hyperliquid"],
    # taker_bps = taker fee in basis points (CHECK YOUR OWN FEE TIER)
    # funding_hours = funding interval of that venue
    "exchanges": {
        "binance": {"symbol": "{b}/USDT:USDT", "taker_bps": 4.5, "funding_hours": 8},
        "bybit": {"symbol": "{b}/USDT:USDT", "taker_bps": 5.5, "funding_hours": 8},
        "okx": {"symbol": "{b}/USDT:USDT", "taker_bps": 5.0, "funding_hours": 8},
        "hyperliquid": {"symbol": "{b}/USDC:USDC", "taker_bps": 4.5, "funding_hours": 1},
    },
    "price_arb": {
        "enabled": True,
        "min_net_edge_bps": 3.0,      # expected profit after ALL fees + buffer
        "take_profit_bps": 2.0,       # close when net PnL >= this (bps of notional)
        "stop_loss_bps": 15.0,
        "max_hold_minutes": 30,
    },
    "funding_arb": {
        "enabled": True,
        "min_apr_pct": 25.0,          # annualised funding differential needed
        "expected_hold_hours": 24,    # used to check funding covers fees
        "min_net_edge_bps": 5.0,
        "exit_apr_pct": 3.0,          # close when differential fades below this
        "max_hold_hours": 72,
        "stop_loss_bps": 25.0,
    },
    "sizing": {
        "notional_usd": 200,          # per leg
        "leverage": 3,
        "max_open_positions": 3,
        "max_total_notional_usd": 1000,
        "min_depth_multiple": 3,      # book must hold N x our size
    },
    "slippage_buffer_bps": 2.0,
    "paper_extra_slippage_bps": 1.0,
    "risk": {"daily_loss_limit_usd": 30, "cooldown_minutes": 10},
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


class Feed:
    """Real market data via ccxt (public endpoints, no keys needed)."""

    def __init__(self, cfg):
        import ccxt

        self.cfg = cfg
        self.ex, self.sym, self.cs, self.fcache = {}, {}, {}, {}
        for name in cfg["enabled_exchanges"]:
            try:
                ex = getattr(ccxt, name)({"enableRateLimit": True})
                ex.load_markets()
            except Exception as e:
                log.warning("skipping %s: %s", name, e)
                continue
            self.ex[name] = ex
            for b in cfg["bases"]:
                s = cfg["exchanges"][name]["symbol"].format(b=b)
                if s in ex.markets:
                    self.sym[(name, b)] = s
                    self.cs[(name, b)] = ex.markets[s].get("contractSize") or 1.0
                else:
                    log.warning("%s has no market %s", name, s)
        if len(self.ex) < 2:
            raise SystemExit("Need at least 2 reachable exchanges.")

    def _funding(self, n, b):
        now = time.time()
        c = self.fcache.get((n, b))
        if c and now - c[0] < FUNDING_REFRESH_S:
            return c[1]
        try:
            r = self.ex[n].fetch_funding_rate(self.sym[(n, b)])
            hourly = float(r["fundingRate"]) / self.cfg["exchanges"][n]["funding_hours"]
        except Exception as e:
            log.debug("funding %s %s: %s", n, b, e)
            return c[1] if c else None
        self.fcache[(n, b)] = (now, hourly)
        return hourly

    def snapshot(self, bases, notional, depth_mult):
        jobs = [(n, b) for b in bases for n in self.ex if (n, b) in self.sym]

        def work(j):
            n, b = j
            try:
                ob = self.ex[n].fetch_order_book(self.sym[(n, b)], 10)
                cs = self.cs[(n, b)]
                if not vwap(ob["asks"], notional * depth_mult, cs) or not vwap(ob["bids"], notional * depth_mult, cs):
                    return j, None
                return j, {
                    "ask": vwap(ob["asks"], notional, cs),
                    "bid": vwap(ob["bids"], notional, cs),
                    "funding_h": self._funding(n, b),
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
        self.rng = random.Random(7)
        self.px = {"BTC": 65000.0, "ETH": 2600.0, "SOL": 150.0}
        self.fund = {}

    def snapshot(self, bases, notional, depth_mult):
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


def find_candidates(cfg, snap):
    pa, fa, slip = cfg["price_arb"], cfg["funding_arb"], cfg["slippage_buffer_bps"]
    out = []
    for base, per in snap.items():
        for x, qx in per.items():
            for y, qy in per.items():
                if x == y:
                    continue
                fees = 2 * (cfg["exchanges"][x]["taker_bps"] + cfg["exchanges"][y]["taker_bps"])
                pe = (qy["bid"] - qx["ask"]) / qx["ask"] * 1e4  # gap we capture if it converges
                if pa["enabled"]:
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
        ex, sym, cs = self.ex[name], self.feed.sym[(name, base)], self.feed.cs[(name, base)]
        amt = float(ex.amount_to_precision(sym, qty_base / cs))
        o = ex.create_order(sym, "market", side, amt, ref_px, {"reduceOnly": True} if reduce else {})
        return float(o.get("average") or o.get("price") or ref_px), float(o.get("filled") or amt) * cs

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

    def save(self):
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

    def exit_reason(self, pos, net, ql, qs, now):
        cfg = self.cfg
        n = pos.notional
        age_h = (now - pos.opened) / 3600
        if self.killed:
            return "kill_switch"
        if pos.strat == "price":
            p = cfg["price_arb"]
            if net <= -p["stop_loss_bps"] / 1e4 * n:
                return "stop_loss"
            if net >= p["take_profit_bps"] / 1e4 * n:
                return "take_profit"
            if age_h * 60 >= p["max_hold_minutes"]:
                return "timeout"
        else:
            p = cfg["funding_arb"]
            if net <= -p["stop_loss_bps"] / 1e4 * n:
                return "stop_loss"
            if ql.get("funding_h") is not None and qs.get("funding_h") is not None:
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
            self.st["day"], self.st["day_pnl"], self.killed = day, 0.0, False

        snap = self.feed.snapshot(cfg["bases"], sz["notional_usd"], sz["min_depth_multiple"])
        if not snap:
            log.warning("no market data this cycle")
            return

        if not self.killed and self.st["day_pnl"] <= -cfg["risk"]["daily_loss_limit_usd"]:
            self.killed = True
            msg = f"KILL SWITCH: daily loss {self.st['day_pnl']:.2f} hit limit. Closing all, no new trades today."
            log.warning(msg)
            notify(msg)

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
            net, _, _, _ = valuation(cfg, pos, ql, qs, cfg["paper_extra_slippage_bps"])
            reason = self.exit_reason(pos, net, ql, qs, now)
            if reason:
                self.close_pos(pos, snap, reason)

        # 2) look for new entries
        cands = find_candidates(cfg, snap)
        for strat in ("price", "funding"):
            best = max((c for c in cands if c.strat == strat), key=lambda c: c.net_bps, default=None)
            if best:
                log.info("best %-7s %s long %s / short %s  net %.1f bps%s", strat, best.base, best.long_ex,
                         best.short_ex, best.net_bps, f"  apr {best.apr:.0f}%" if strat == "funding" else "")

        if not self.killed:
            for c in sorted((c for c in cands if passes(cfg, c)), key=lambda c: -c.net_bps):
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
                    self.positions.append(pos)
                    self.record("OPEN", pos, "", f"net_edge={c.net_bps:.1f}bps")
                    msg = f"[{'LIVE' if self.broker.live else 'PAPER'}] OPEN {c.strat} {c.base} " \
                          f"L:{c.long_ex} S:{c.short_ex} ${sz['notional_usd']} edge={c.net_bps:.1f}bps"
                    log.info(msg)
                    notify(msg)
        self.save()

    def run(self, max_loops=None):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--demo", action="store_true", help="synthetic data, offline logic test")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--yes-real-money", action="store_true")
    ap.add_argument("--max-loops", type=int)
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config(a.config)

    if a.demo:
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
    signal.signal(signal.SIGINT, lambda *_: setattr(bot, "running", False))
    signal.signal(signal.SIGTERM, lambda *_: setattr(bot, "running", False))
    bot.run(a.max_loops)


if __name__ == "__main__":
    main()
