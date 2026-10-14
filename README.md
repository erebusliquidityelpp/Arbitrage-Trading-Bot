# Arbitrage Bot: convergence trade between exchanges, paper-first

Trades a perpetual futures pair as a delta-neutral position: **long on the cheaper exchange, short on the richer one**, same coin, same size. No coins move between exchanges; only margin sits on each one.

**Strategy (the only one).** Every coin has a *normal* price gap between the two exchanges (its "basis"). The bot learns each coin's normal gap and its normal wiggle, and opens a trade only when the gap is stretched at least `z_min` (3) standard deviations beyond normal AND the expected profit still clears every cost (both exchanges' fees, both spreads going in and out, slippage). It closes when the gap snaps back (take profit), moves further against it (stop loss, measured after entry) or after 15 minutes. The funding-rate strategy was removed.

## Run

```bash
pip install -r requirements.txt
python arb_bot.py --demo --max-loops 300   # offline logic test, no network
python arb_bot.py                          # PAPER: live prices, simulated fills
```

Outputs: `trades.csv` (every open/close with PnL), `state.json` (open positions survive restarts).

## Coins: liquid only

`bases: liquid` makes the bot, at every startup, take the coins listed on both Binance and Hyperliquid, read their 24h volume on **both**, and keep the `top_liquid_n` (20) with the highest volume on the thinner exchange (at least `min_volume_usd` on each). The list and volumes are printed in the startup log (`LIQUID coin list: ...`) and shown by `/coins`. If volumes cannot be read it falls back to a built-in list of major coins. Scaled names (1000PEPE = kPEPE) are matched automatically. Set `bases: auto` for every shared coin (thin coins lost money in testing) or give your own list.

Every scan costs one bulk price call per exchange; order books are only fetched for the `refine_top_n` coins whose gap is most stretched, plus open positions.

## Record and replay (tune on real data)

Every scan is saved to `/data/rec/YYYY-MM-DD.jsonl.gz` (about 10 MB a day for 20 coins, the last 7 days are kept): mark prices of every coin on both exchanges, plus real bid/ask for coins whose order book was fetched. After a day or two you can test settings on that exact data in seconds, with no waiting:

```
python arb_bot.py --replay /data/rec
python arb_bot.py --replay /data/rec --grid price_arb.z_min=2.5,3,3.5 --grid price_arb.min_net_edge_bps=2,4,6
```

The second command tries every combination and prints trades, win rate, average and total profit per row, plus the exit reasons and results by coin for the best one. Coins without a recorded order book use an assumed spread (`--assumed-spread-bps`, default 1), so treat the numbers as an estimate. The best row of a sweep is partly luck: only trust a setting that also works on a different day than the one you tuned it on.

## Health alerts

A watchdog sends a Telegram warning if no scan completes for 2 minutes (exchange API down or the bot stuck) and tells you when scanning recovers. A short daily report (profit for the day, all-time profit, open positions) is sent at 00:00 UTC.

## Weekends

`schedule.pause_weekends: true` stops NEW trades on Saturday and Sunday (timezone set by `schedule.timezone`, default UTC, e.g. `Asia/Karachi`). Open positions are still managed and closed normally, you get a Telegram message when the pause starts and ends, and `/status` shows `WEEKEND PAUSE`.

## Website (ELPP Data Desk) - "Arbitrage Bot" section

The bot serves a small **read-only, key-protected** API, and the Data Desk (ELPP-IRCS-Scanner service) shows it as its own page, `/arb`, with: today / 7-day / all-time profit, a profit curve, daily bars, open positions, full trade history with filters, and results per coin. It is **master account only** (students are blocked by the server). Nothing else from the bot is exposed: no exchange keys, no controls.

Setup (3 variables, then redeploy both services):
1. Make one long random secret (any 32+ characters, e.g. `python -c "import secrets;print(secrets.token_urlsafe(32))"`).
2. **Arbitrage bot service** variables: `ARB_API_KEY` = the secret, `ARB_API_PORT` = `8080`.
3. **ELPP-IRCS-Scanner service** variables: `ARB_API_KEY` = the same secret, and `ARB_BOT_URL` = `http://<arb-service-private-name>.railway.internal:8080`.
   Copy the private name from the arbitrage service: Settings > Networking > Private Networking. If the two services are in different Railway projects, generate a public domain on the arbitrage service instead and use `https://<that-domain>` (the API still needs the key).
4. Sign in to the Data Desk as master. A small "Arbitrage Bot" button appears at the bottom right of the dashboard, or open `/arb` directly.

If the page says "not reachable", the message names what is missing. The arbitrage bot must be running for the page to have data.

## Telegram (alerts + commands)

Create a **separate bot** with @BotFather (not your ELPP bot, two programs cannot poll the same token). Set `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` as variables. The bot only obeys messages from that chat ID.

| Command | What it does |
|---|---|
| `/status` | Mode, state, uptime, last scan, open positions, today / all-time PnL |
| `/positions` | Open positions with live "if closed now" PnL |
| `/pnl` | Realised PnL: today, last 7 days, all time, win rate |
| `/trades [n]` | Last n closed trades (default 5, max 20) |
| `/coins` | PnL per coin + coins currently being scanned |
| `/best` | The most stretched gaps now (z-score, expected net) vs. the entry thresholds |
| `/config` | Current strategy settings |
| `/pause` / `/resume` | Block / allow NEW trades (open ones are still managed) |
| `/closeall confirm` | Close every open position at market |

Automatic alerts: every open, every close (coin, strategy, exchanges, PnL, reason), kill switch, and failed live orders. Commands sent while the bot was down are discarded on restart.

Leave it in paper mode for **at least 1-2 weeks**. Compare paper PnL against reality before risking money. Paper fills assume top-of-book depth and cannot capture queue position, partial fills or API outages.

## Built-in safety

- Order-book depth check (book must hold 3x your size) and VWAP pricing
- Entry filters: the gap must be stretched beyond the coin's own normal level (`z_min`) by enough to cover fees + buffer + entry and exit spreads; book price and mark price must agree; thin books (spread above `max_spread_bps`) are skipped; an opportunity must repeat `confirm_scans` scans in a row; a coin is learned for `warmup_samples` scans (~15 min) after every fresh start before it can trade
- A coin that loses 2 trades in a row is banned for 24 hours
- Max open positions, max total notional, one position per coin, cooldown after each close
- Per-trade stop loss (measures price/funding loss only, fees excluded), timeouts, daily loss kill switch (closes everything, pauses until next UTC day)
- If one leg of an entry fails live, the other leg is flattened immediately
- Close failures raise an alert; check the exchanges manually when that happens

## Going live (only after paper results look real)

1. Set `mode: live` in `config.yaml`
2. Export API keys as environment variables: `BINANCE_KEY/SECRET`, `BYBIT_KEY/SECRET`, `OKX_KEY/SECRET/PASSWORD`, `HYPERLIQUID_WALLET/PRIVATE_KEY`
3. **Trade-only keys. Never enable withdrawals.** Use IP whitelisting where offered.
4. Fund each exchange with margin, start with a tiny `notional_usd` (e.g. 20-50)
5. `python arb_bot.py --live --yes-real-money`

The live broker is **untested against real exchanges**. I could only test the logic on simulated data. Test with minimum sizes first and expect to fix exchange-specific quirks (contract sizes, margin modes, Hyperliquid order params).

## Honest limits

- **Fees decide everything.** At ~4.5 bps taker per leg, a round trip costs ~18 bps before spreads. A convergence trade on a liquid coin therefore only fires when the gap is stretched by roughly 25 bps or more, which happens during fast market moves, not all day. Expect FEW trades. If it makes none for hours, that is the filter working, not a bug.
- It is a taker strategy. Professional desks earn their edge with maker/rebate fee tiers, colocated servers and pre-funded accounts on every venue. Lower your real fee tier (Binance BNB discount / VIP, Hyperliquid fee tiers) and set `taker_bps` to it.
- The normal gap is learned from live data, so after a restart a coin needs ~15 minutes before it can trade again (the learned values are saved and reused when the volume is intact).
- Paper fills assume top-of-book depth and cannot capture queue position, partial fills or API outages.
- No bot "does everything" risk-free. Keep paper mode for 1-2 weeks of positive results before considering live.
