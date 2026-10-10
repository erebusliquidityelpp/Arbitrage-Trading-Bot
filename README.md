# Arbitrage Bot (price + funding), paper-first

Trades perpetual futures as a delta-neutral pair: **long on exchange X, short on exchange Y**, same coin, same size. No coins move between exchanges; only margin sits on each one.

| Strategy | Enters when | Exits when |
|---|---|---|
| `price` | Y's bid is above X's ask by more than all fees + buffer | gap converges (take profit), stop loss, timeout |
| `funding` | funding paid to the short side beats what the long side pays, enough to cover fees over the expected hold | differential fades, stop loss, timeout |

## Run

```bash
pip install -r requirements.txt
python arb_bot.py --demo --max-loops 300   # offline logic test, no network
python arb_bot.py                          # PAPER: live prices, simulated fills
```

Outputs: `trades.csv` (every open/close with PnL), `state.json` (open positions survive restarts).

## Coins (automatic)

`bases: auto` in `config.yaml` makes the bot, at every startup, load the full market lists from Binance and Hyperliquid and trade **every perpetual that is listed on both**. Nothing is hand-picked, so nothing is missed, and new listings are picked up on the next restart. The full list is printed in the startup log (`AUTO coin list: N coins ...`) and is available any time with `/coins`.

- Scaled names are matched and converted automatically (Binance `1000PEPE` = Hyperliquid `kPEPE`; `1000000MOG`, `1MBABYDOGE` etc.), and prices are compared per 1 coin. `1INCH` is correctly treated as a normal coin.
- Only USDT-margined (Binance) and USDC-margined (Hyperliquid) perpetuals count. Quarterly futures, inactive markets and coins listed on only one exchange are skipped. Hyperliquid HIP-3 markets (stocks, etc.) are not included.
- Funding intervals are read per coin (Binance uses 8h, 4h or 1h depending on the coin).
- To exclude a coin, add it to `exclude_coins`. To go back to a hand-picked list, set `bases: [BTC, ETH, ...]`.

How it scans hundreds of coins without hitting rate limits: every cycle it makes ONE bulk call per exchange (mark price + funding for all coins), ranks every coin by edge, then fetches full order books only for the top `refine_top_n` (12) coins and any coin with an open position. A trade is only ever opened on order-book prices with depth checked. Gaps above `max_price_gap_bps` (100) are ignored as likely data errors.

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
| `/best` | Top opportunities now vs. the entry thresholds |
| `/config` | Current strategy settings |
| `/pause` / `/resume` | Block / allow NEW trades (open ones are still managed) |
| `/closeall confirm` | Close every open position at market |

Automatic alerts: every open, every close (coin, strategy, exchanges, PnL, reason), kill switch, and failed live orders. Commands sent while the bot was down are discarded on restart.

Leave it in paper mode for **at least 1-2 weeks**. Compare paper PnL against reality before risking money. Paper fills assume top-of-book depth and cannot capture queue position, partial fills or API outages.

## Built-in safety

- Order-book depth check (book must hold 3x your size) and VWAP pricing
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

- **Fees decide everything.** At ~5 bps taker per leg, a price-arb round trip costs ~20 bps. On BTC/ETH the cross-exchange gap rarely exceeds that, and fast market makers take it first. Price arb works better with maker/low-fee tiers or on volatile altcoins. Edit `taker_bps` to your real tier.
- **Funding arb is the steadier one**, but funding can flip, and exchanges can liquidate a leg if price moves hard. Keep leverage low and margin topped up.
- Funding PnL is estimated from funding rates, not read from exchange settlements.
- No bot "does everything" risk-free. Watch it, keep the daily loss limit, and only use money you can lose.
