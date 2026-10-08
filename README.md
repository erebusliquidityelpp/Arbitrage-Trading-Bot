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

Outputs: `trades.csv` (every open/close with PnL), `state.json` (open positions survive restarts). Optional Telegram alerts: set `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`.

Leave it in paper mode for **at least 1-2 weeks**. Compare paper PnL against reality before risking money. Paper fills assume top-of-book depth and cannot capture queue position, partial fills or API outages.

## Built-in safety

- Order-book depth check (book must hold 3x your size) and VWAP pricing
- Max open positions, max total notional, one position per coin, cooldown after each close
- Per-trade stop loss, timeouts, daily loss kill switch (closes everything, pauses until next UTC day)
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
