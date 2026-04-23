# Smart-Money Trailing: Evaluation & Improvement Plan

_2026-04-20_

## Evaluation: end-to-end `smart_money` flow

**Two-stage flow:**

1. **Identification** — `polybot.smart_wallets.cli run` (weekly):
   leaderboard + Goldsky seed → Data API enrich → ledger metrics → filter →
   dual-archetype score → sybil collapse → writes
   `smart_wallets_signal.json` / `smart_wallets_closer.json`.
2. **Trailing** — `SmartMoneyStrategy.evaluate()` (every 30s per market):
   loads JSON, fetches Goldsky firehose for last 60min, scans for whale BUYs
   on the market's outcomes, emits a flat $10 limit order.

The identification side is solid post-refactor. The **trailing side has real
problems** that mean we're not actually following whales well.

## What's broken or wasteful

| # | Issue | Severity |
|---|---|---|
| 1 | **No exit logic** — strategy only fires on whale BUYs. When the whale closes, the bot keeps the position. "Trail" is one-shot entry, not mirror. | **Critical** |
| 2 | **`_load_smart_wallets()` runs inside `evaluate()` per market per cycle** — reads + parses JSON + re-filters stale/score hundreds of times per cycle. Pure waste. | **Critical** |
| 3 | **`min_wallet_buys: 1` in committed config (main `a342902`)** — single whale triggers entry, defeating the confluence + weighted-score design. | **Critical** |
| 4 | **Goldsky query pulls the full firehose** — no `taker_in: [...wallets]` filter. We fetch tens of thousands of events per hour, discard >99% in Python. GraphQL supports address filters. | High |
| 5 | **No per-market cooldown / open-position dedupe for smart_money** — same confluence persists across cycles → repeated orders until risk caps reject. | High |
| 6 | **Flat `$10` sizing** regardless of whale score, confluence weight, or the whale's own position size. Score is used only as a threshold gate. | High |
| 7 | **`max_entry_price: 0.95`** is effectively uncapped → we chase post-move fills where edge is gone. | High |
| 8 | **YAML fallback wallets are dead code** after first successful pipeline run (`_load_smart_wallets` returns dynamic → YAML never consulted). | Medium |
| 9 | **Signal archetype only by default** (closer is only the fallback when signal.json is missing). A wallet in `closer` but not `signal` is ignored even when it's closing a position we'd want to mirror. | Medium |
| 10 | **No trailing-specific backtest.** `backtest.py` replays cohort forward-PnL; it doesn't simulate what the *bot* would have done (entry timing, size, exits). All trailing params are currently untuned guesses. | Medium |
| 11 | **Per-cycle rebuild of `buyers_by_token`** is O(markets × events). Could be inverted to event→market once per cycle. | Low |

## Prioritized plan

### P0 — make trailing actually "follow"

1. **Add SELL detection to `SmartMoneyStrategy`**: when a wallet whose BUY
   we mirrored SELLS its side, emit a close signal. Needs
   `opened_by_smart_money` tags persisted in `TradeTracker`, keyed by wallet.
2. **Cache `_load_smart_wallets()` at cycle-start** (`reset_cycle_cache`
   clears it); mtime-check to refresh within a week if pipeline reruns.
3. **Restore `min_wallet_buys ≥ 2`** and enforce `min_confluence_weight`
   (weighted sum of whale scores, not count).
4. **Tighten entry**: set `max_entry_price: 0.75` and add a "within N bps
   of whale's fill price" guard using the Goldsky event's `price`.

### P1 — efficiency

5. **Filter Goldsky at query time**:
   `where: {taker_in: [...wallets], timestamp_gt: ...}` and
   `where: {maker_in: [...]}` (two queries, merged). Shrinks payload
   50–200×.
6. **Invert the per-cycle loop**: one pass over events →
   `dict[conditionId, list[whale_buy_events]]`; per-market lookup becomes
   O(1).
7. **Union `signal` + `closer` archetypes** at load time; use max of the
   two per-wallet scores.

### P2 — sizing + exits

8. **Confluence-proportional sizing**:
   `size = base_usdc * min(confluence_weight_sum, cap)`; never exceed
   `max_position_usdc`.
9. **Per-market cooldown** (e.g. 15 min) after any smart_money signal
   fires on that market.
10. **Persist `smart_money_positions.jsonl`** keyed by `(market, token)`
    with opening whale(s) + entry ts; exit on any of:
    (a) opening whale SELLs, (b) TTL expired, (c) stop-loss (existing).

### P3 — measurement

11. **Forward-trail backtest**: replay the last week of Goldsky events,
    simulate `evaluate()` calls at 30s cadence with the same config,
    mark-to-market at resolution. Compare bot-PnL vs. cohort-PnL. This is
    the only principled way to tune `lookback_minutes`,
    `min_confluence_weight`, `max_entry_price`.
12. **Dashboard page 4: trail performance section** — signals fired,
    fills, PnL per trailed wallet.

## Recommendation

P0 items #1 and #2 explain most of "trailing isn't efficiently happening."
#3 is a config-only change and takes seconds. #5 will materially cut
Goldsky egress.
