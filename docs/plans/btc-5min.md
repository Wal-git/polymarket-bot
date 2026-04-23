# BTC 5-Minute Signal Engine

_2026-04-23_

## Strategy

Trade Polymarket's BTC 5-minute UP/DOWN markets using two-signal confluence:

1. **Price divergence**: Binance AND Coinbase both >$50 above/below the market's "Price to Beat"
   (set by Chainlink at slot open). 94% Chainlink follow-through when both exchanges agree.
2. **Order-book imbalance**: Top-10 depth bid/ask ratio ≥1.8 (bullish) or ≤0.55 (bearish),
   measured in the 30–90s window after market open (smart-money entry period).

Entry: 60–180s after market open. Exit: 75c profit target, 35c stop loss, or hold to resolution
if <60s remain. Kelly sizing (quarter-Kelly). ~85% of 5-min windows are skipped.

## Repo layout

```
polymarket-bot/
├── src/polybot/
│   ├── engine/         discovery, lifecycle, scheduler
│   ├── feeds/          btc_price, orderbook_ws
│   ├── signals/        imbalance, divergence, combiner
│   ├── execution/      entry, exit, sizing
│   ├── account/        balance
│   ├── backtest/       harness, data/
│   ├── agents/         merged from agents/ repo (optional, [llm] extra)
│   ├── client/         clob, gamma (unchanged)
│   ├── auth/           wallet (unchanged)
│   ├── safety/         risk_manager (unchanged)
│   └── monitoring/     tracker (unchanged)
├── tools/polymarket-cli/   Rust CLI source (bash scripts/install_cli.sh to build)
├── reference/trade-engine-ts/  frozen TS reference (unbuilt)
└── config/default.yaml
```

## Verification sequence

1. **Unit tests**: `pytest tests/ -v` — imbalance, divergence, combiner, sizing, lifecycle
2. **Backtest**: `polybot backtest --days 30` — requires poly_data CSVs in `src/polybot/backtest/data/`
3. **Paper trade**: 7 days with `dry_run: true`. Assert ~85% skip rate, no runtime errors.
4. **Live canary**: Flip `dry_run: false`, `max_trade_usdc: 10`, `daily_loss_limit_usdc: 50`. Run 3 days.
5. **Scale-up**: Raise `max_trade_usdc` 2× every 3 days after positive canary.

## Open risks

- Chainlink `price_to_beat` comes from Gamma API `startPrice` / `openPrice`. If Polymarket's API
  key changes, discovery will return 0 and all slots will be skipped.
- WebSocket reconnect backoff capped at 30s. Extended outages during a 5-min slot will miss entry.
- Methodology decay: article warns edges compress as more bots enter.
