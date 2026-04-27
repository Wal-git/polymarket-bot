# Signal Improvements & Data Source Expansion

_2026-04-27_

## Context

After 9 days of live trading (45 unique trades, 75.6% win rate, **−$45.61 net P&L**), data
analysis revealed the bot is in an asymmetric-payoff trap: average entry $0.78 needs ~78%
WR to break even; we hit 75.6%. Calibration gap between modeled EV (+$200) and realized
P&L (−$46) is **−$246**. See companion analysis for full breakdown.

This plan scopes the changes the user authorized: signal improvements **A, B, C, D, F**
(skipping the imbalance-related changes), and external data sources **#1–3** (high value,
low cost) and **#7–9** (lower priority). Medium-value items #4–6 are out of scope.

---

## Signal improvement work

### A. Empirical confidence calibration

**Problem:** Current formula `0.6 + |Δ_binance|/500 + |Δ_coinbase|/500` (capped at 0.95)
claims 95% confidence when realized win rate is 79%. Kelly sizing then over-bets.

**Scope:**
1. Add `src/polybot/signals/calibration.py` with a `lookup_win_rate(features) -> float`
   function that reads from `data/calibration_table.json`.
2. Buckets to use as the lookup key (start simple):
   - `delta_bucket`: `<75 | 75–100 | 100–150 | 150–200 | 200–300 | 300+`
   - `entry_bucket`: `<0.70 | 0.70–0.80 | 0.80–0.85 | 0.85–0.90 | 0.90+`
   - `hour_utc`: 0–23
3. Add `scripts/build_calibration.py`: reads `evaluations.jsonl` + `results.jsonl`,
   joins by slug, computes empirical win rate per (delta_bucket × entry_bucket × hour)
   bucket, with **Laplace smoothing** (`(wins+1)/(trials+2)`) and a **min-N=5** fallback
   to coarser buckets when the cell is sparse. Writes `data/calibration_table.json`.
4. Update `src/polybot/signals/combiner.py:113` to call `lookup_win_rate(...)` instead
   of the linear formula.
5. Add config knob `signals.calibration.fallback_confidence: 0.75` for cold-start.
6. Re-run `scripts/build_calibration.py` weekly via a cron entry (or inline at bot
   start-up — read once, cache).

**Files touched:**
- new: `src/polybot/signals/calibration.py`, `scripts/build_calibration.py`,
  `data/calibration_table.json`, `tests/signals/test_calibration.py`
- modified: `src/polybot/signals/combiner.py`, `config/default.yaml`

**Validation:** Backtest the calibration table by leaving out the last week and
predicting it; report Brier score vs current formula. Calibration table from 45 trades
is *very* sparse — expect heavy reliance on the smoothing fallback initially. Re-train
weekly.

**Effort:** ~half day. **Risk:** medium (changes core sizing math — gate behind a feature
flag).

---

### B. Max-divergence cutoff

**Problem:** Trades with `max(|Δ_binance|, |Δ_coinbase|) > 200` are net negative
(−$64 over 8 trades). The market has already over-priced the move; mean-reversion
within 5 min eats the entry premium.

**Scope:**
1. Add `signals.divergence.max_gap_usd: 200.0` to `config/default.yaml`.
2. In `combiner.py`, after computing `binance_delta`/`coinbase_delta`, reject when
   `max(abs(binance_delta), abs(coinbase_delta)) > max_gap_usd` with
   `reject_reason="divergence_too_large"`.
3. Lower `fast_pass_usd` from 150 → 125 (fast-pass should be a *lower* bar, not catch
   tail trades).
4. Emit the rejection in `evaluations.jsonl` so we can monitor how often it triggers.

**Files touched:** `src/polybot/signals/combiner.py`, `config/default.yaml`,
`tests/signals/test_combiner.py`.

**Validation:** Replay the 8 over-cap historical trades — confirm they would all be
rejected. Counter-factual P&L on this dataset: −$46 → +$18.

**Effort:** ~30 min. **Risk:** low.

---

### C. Dynamic profit-take + reversal stop

**Problem:** All 45 trades exited via `HOLD_TO_RESOLUTION`. Profit target 0.75 is
*below* avg entry 0.78 — can never fire. Stop loss 0.35 is too far from any realistic
5-min trajectory.

**Scope:**
1. **Dynamic profit-take.** In `src/polybot/execution/exit.py:25`, replace the static
   `profit_target` with `entry_price + profit_target_delta`, where
   `profit_target_delta` defaults to `0.10`. Add `signals.exit.profit_target_delta:
   0.10` to config. Also add an absolute ceiling: sell unconditionally when
   `current_bid >= 0.95` (lock near-certain wins early; protects against late reversal).
2. **Reversal stop.** Add a price-feed check inside `monitor_position`: if BTC spot
   (Binance) returns to within `reversal_band_usd` (default 25) of `slot.price_to_beat`
   *after* having exceeded `min_gap_usd`, exit at the current bid. Fetches reuse
   existing `fetch_btc_prices` — poll every 2s alongside the existing bid check.
3. **Keep the existing 0.35 stop** as a backstop but raise it to `0.45` so it actually
   fires when a position has clearly gone wrong (entry 0.78 → 0.45 is a real move).

**Files touched:** `src/polybot/execution/exit.py`, `config/default.yaml`,
`tests/execution/test_exit.py`.

**Validation:** Simulate exit logic on the 11 historical losers — does the reversal
stop catch any before resolution? Bound `reversal_band_usd` from above by `min_gap_usd`
(otherwise the entry band and exit band overlap and you'll churn).

**Effort:** ~1 day (need to wire spot-price polling into the exit loop). **Risk:** medium
— exit changes affect every active position, test thoroughly in dry-run first.

---

### D. Per-trade max loss cap

**Problem:** Three losers cost $30, $66, $93 each. Kelly + 0.95 confidence + $200 cap
allows up to $200 per trade. Until calibration (item A) lands and proves trustworthy,
the per-trade *loss* (not stake) needs an explicit ceiling.

**Scope:**
1. Add `sizing.max_loss_usdc: 25.0` to `config/default.yaml`.
2. In `src/polybot/execution/sizing.py:`, after computing `raw_size`, also compute
   `max_size_loss_capped = max_loss_usdc / entry_price` (the position size where a full
   loss = `max_loss_usdc`). Final size = `min(raw_size, max_size_loss_capped, max_usdc)`.
3. The existing `max_trade_usdc: 200` becomes the upper bound on stake; the new cap is
   the upper bound on *loss*. Keep both.

**Files touched:** `src/polybot/execution/sizing.py`, `config/default.yaml`,
`tests/execution/test_sizing.py`.

**Validation:** With `max_loss_usdc=25`, no historical trade would have lost > $25.
Counterfactual P&L: the 11 losers cap at −$25 each → loss side capped at −$275 vs
actual −$381, freeing ~$106. Note: this also caps *upside*, since smaller stake = smaller
win. Check the historical winner side: at avg entry 0.78, $25 loss-cap implies max stake
$32. Most trades are already $30, so impact on wins is small.

**Effort:** ~1 hour. **Risk:** low. **Remove this cap once calibration is mature.**

---

### F. Time-of-day feature logging (no filter yet)

**Problem:** Hours 02, 05, 15, 16, 19 UTC are net-negative; 12, 14, 22 net-positive.
Sample size per hour is too small to filter on (1–11 trades). But we can start
recording so the calibration table (item A) picks it up automatically.

**Scope:**
1. `hour_utc` is already the third bucket dimension in item A's calibration table —
   nothing extra to do code-wise once A lands.
2. Add a dashboard/logging widget summarizing P&L per hour-of-day so we can eyeball
   regime drift.

**Files touched:** `src/polybot/dashboard/data_loader.py` (add hour-of-day breakdown).

**Effort:** ~1 hour. **Risk:** none (read-only).

---

## External data source work

### #1. Chainlink BTC/USD oracle feed (CRITICAL GAP)

**Why first:** The strategy thesis is "Chainlink lags exchange prices." We currently
infer the lag — we never read Chainlink directly. The aggregator feed tells us
*exactly* what the market will settle on and *when* the next round will hit.

**Scope:**
1. Add `src/polybot/feeds/chainlink.py` with:
   - `fetch_chainlink_btc(rpc_url) -> ChainlinkRound`: call `latestRoundData()` on the
     Polygon BTC/USD aggregator (`0xc907E116054Ad103354f2D350FD2514433D57F6f` on
     Polygon mainnet — verify before deploying).
   - Returns `(answer, updated_at, round_id)`.
2. Wire into `combiner.py`: log `chainlink_price`, `chainlink_lag_seconds`,
   `chainlink_vs_exchange_gap` in every evaluation. **Do not gate on it yet** — collect
   data first.
3. After 1 week of data: add `signals.divergence.require_chainlink_lag_s: 30` —
   only fire when Chainlink is at least 30s old (i.e., overdue for an update — the
   lag we're trying to exploit).
4. Use a dedicated RPC (Alchemy/Infura free tier is fine for ~one read per 10s).
   Reuse existing RPC config if the bot already has one for redemption.

**Files touched:** new `src/polybot/feeds/chainlink.py`, `src/polybot/signals/combiner.py`,
`config/default.yaml`, `tests/feeds/test_chainlink.py`.

**Effort:** ~1 day. **Risk:** low. **Cost:** free RPC tier.

**Open questions to resolve before coding:**
- Confirm the actual aggregator address Polymarket uses for resolution (check the market
  contract or Polymarket docs — could be a different proxy).
- Confirm the round cadence (BTC/USD on Polygon updates ~hourly or on >0.5% deviation —
  our 5-min slots may often have stale rounds, which is exactly the opportunity).

---

### #2. Additional spot exchanges (Kraken, Bitstamp, OKX)

**Why:** Currently 2 exchanges → divergence. With 4–5 sources we can require
N-of-M agreement and filter regional outliers.

**Scope:**
1. Extend `src/polybot/feeds/btc_price.py` with `_fetch_kraken`, `_fetch_bitstamp`,
   `_fetch_okx`. APIs:
   - Kraken: `https://api.kraken.com/0/public/Ticker?pair=XBTUSD`
   - Bitstamp: `https://www.bitstamp.net/api/v2/ticker/btcusd/`
   - OKX: `https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT`
2. Update `BtcPrices` model (`src/polybot/models/btc_market.py`) to a flexible
   `dict[str, float]` of exchange→price, instead of two named fields.
3. In `combiner.py`, replace the `binance > min_gap AND coinbase > min_gap` check with
   "≥ N of M exchanges past the gap." Add `signals.divergence.min_agreement: 3`,
   default 3-of-4.
4. Treat partial fetch failures gracefully — exchange goes down → drop it from the
   agreement count, do not abort.

**Files touched:** `src/polybot/feeds/btc_price.py`,
`src/polybot/models/btc_market.py`, `src/polybot/signals/divergence.py`,
`src/polybot/signals/combiner.py`, `config/default.yaml`,
`tests/feeds/test_btc_price.py`, `tests/signals/test_divergence.py`.

**Effort:** ~1 day. **Risk:** medium — `BtcPrices` is referenced in many places, schema
change ripples. Consider a backwards-compat shim (`prices.binance` → `prices.get('binance')`).

---

### #3. Binance Futures funding rate + mark price

**Why:** Futures often lead spot on impulsive moves. A flipping funding rate at the
slot boundary is a clean reversal signal.

**Scope:**
1. Add `src/polybot/feeds/binance_futures.py`:
   - `fetch_premium_index() -> FuturesSnapshot`: REST `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT`
     gives `markPrice`, `indexPrice`, `lastFundingRate`, `nextFundingTime`.
2. Log `mark_price`, `funding_rate`, `funding_until_next_s`, `mark_minus_spot_usd` in
   every evaluation (data collection only at first).
3. After 1 week: add an optional gate. Two candidates to test:
   - **Funding skew filter**: skip UP signals when `funding_rate > 0.0001` (longs already
     paid up — reversal risk high).
   - **Mark/spot divergence**: if `mark_price > index_price + 50` and we're firing UP,
     allow; if futures *disagrees* with spot direction, skip.

**Files touched:** new `src/polybot/feeds/binance_futures.py`,
`src/polybot/signals/combiner.py`, `config/default.yaml`,
`tests/feeds/test_binance_futures.py`.

**Effort:** ~half day to wire data collection; gate logic comes later. **Risk:** low.
**Cost:** free.

---

### #7. VIX / DXY / S&P futures (lower priority — data collection only)

**Why:** BTC correlates with risk-on/off. Probably noise at 5-min cadence, but worth
logging once to find out.

**Scope:**
1. Add `src/polybot/feeds/macro.py` polling at slow cadence (e.g. once per minute, not
   per second):
   - VIX: Yahoo Finance unofficial API or CBOE delayed feed.
   - DXY: Yahoo `DX-Y.NYB`.
   - ES/NQ futures: Yahoo `ES=F` / `NQ=F` or `tradingeconomics` (delayed is fine).
2. Log `vix`, `dxy`, `es_pct_change_1h` in every evaluation. **Logging only — no
   gating.** After 4+ weeks of data, look for any correlation with win rate by hour.
3. Cache aggressively (1-min TTL) — these are slow-moving and Yahoo will rate-limit.

**Files touched:** new `src/polybot/feeds/macro.py`,
`src/polybot/signals/combiner.py`, `config/default.yaml`.

**Effort:** ~half day. **Risk:** low (read-only data collection). **Cost:** free, but
unofficial APIs are fragile — wrap aggressively in try/except.

**Note:** If after 4 weeks of data there is no signal, **delete this code**. Don't let
it accrete as dead weight. Set a calendar reminder to review.

---

### #8. Sentiment / news feed (lower priority — defer)

**Why:** Could catch regime breaks (CPI prints, Fed surprises). But hard to integrate
at 5-min cadence — the news arrives, the move happens, you missed it.

**Scope:** Defer until items A–F and #1–3 are stable and earning. Then revisit with a
specific hypothesis (e.g. "block all signals within ±10 min of a high-impact economic
release"), not as a vague "add sentiment."

**Recommendation:** **Do not start this until calibration shows the underlying
strategy is profitable.** Adding noisy features to a losing strategy is a way to
overfit yourself out of finding the real bug.

---

### #9. On-chain stablecoin mints (lower priority — defer)

**Why:** Large USDT/USDC mints sometimes precede BTC rallies. But: lots of false
positives, and the lag from mint → price action is hours-to-days, not minutes.

**Scope:** Defer. **Wrong timescale for a 5-min strategy.** Mention it again only if
we expand to longer-horizon markets.

---

## Recommended ordering

| Phase | Items | Why |
|---|---|---|
| **1 — Stop the bleeding** (this week) | B, D, C | Each is small, isolated, and fixes the asymmetric-payoff problem. Done before any data work. |
| **2 — Fix the data integrity bug** (this week) | (companion task — see analysis) | The `confidence=0` rows in `results.jsonl` are masking calibration. Fix the writer in `monitoring/tracker.py` or wherever results are emitted. |
| **3 — Get the right data** (next 1–2 weeks) | #1, #2, #3 | Chainlink is the priority. More exchanges + futures expand the divergence signal. **Collect data, don't gate yet.** |
| **4 — Calibrate** (after 2+ weeks of richer data) | A, F | Empirical calibration is only as good as the data feeding it. Don't build the table on noisy/incomplete features. |
| **5 — Defer / kill** | #7, #8, #9 | Log #7 in the background, ignore #8/#9 until the strategy is in the black. |

---

## What this plan does NOT include (per user direction)

- Imbalance changes (item E from analysis) — orderbook imbalance logic is left as-is.
- Medium value, medium cost data sources (#4 spot trade tape, #5 Polymarket smart-money
  trade tape for BTC slots, #6 CME basis) — deferred to a later plan.

---

## Success criteria

After Phase 1+2 complete (items B, C, D, results-bug fix):
- Net P&L positive over a rolling 30-trade window
- Average loss < average win (currently $34.68 vs $9.88)

After Phase 3+4 complete (items #1–3, A):
- Calibrated confidence has Brier score < 0.20 vs ~0.30 today
- Modeled-EV vs realized-P&L gap < 20% (currently 4×)
