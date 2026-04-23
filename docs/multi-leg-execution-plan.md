# Multi-Leg Execution — Next Session Plan

**Goal:** Wire live order placement for covered calls (buy-write) so
`CoveredCallScreener.execute()` stops being a no-op. This is the first
concrete step toward closing the wiki's "No live options execution" gap.

Scope is intentionally narrow: covered calls only. Verticals / condors /
butterflies come later, reusing the same plumbing.

---

## Where we left off

- `src/schwabagent/strategies/covered_call_screener.py` — scan produces
  ranked opportunities with `call_symbol`, `strike`, `expiration`, `dte`,
  `call_bid`, `call_ask`, `call_premium`, `price` (spot). `execute()`
  returns `None`.
- `src/schwabagent/options.py` — `covered_call()` builds a 2-leg payoff
  model (long stock + short call) for analysis. **Not an order builder.**
- `src/schwabagent/schwab_client.py::place_order()` — equity-only. Uses
  `schwab.orders.equities` builders, buffered LIMIT pricing, DAY/SEAMLESS
  duration/session. Returns `{status, order_id, …}`.
- `CoveredCallScreener` sits in STRATEGIES but is scan-only (no
  `LIVE_COVERED_CALL_SCREENER` flag in `config._STRATEGY_LIVE_FLAGS`).
- Event blackout gate in `trading_rules._check_event_blackout` already
  BUY-gates the equity leg; earnings + dividend calendars feed it.

---

## What schwab-py gives us

Confirmed in `.venv/lib/python3.11/site-packages/schwab/orders/`:

| Module | Useful symbols |
|--------|---------------|
| `equities.py` | `equity_buy_limit(sym, qty, price)`, `equity_buy_market` |
| `options.py`  | `option_sell_to_open_limit(sym, qty, price)`, `option_buy_to_close_limit`, plus `OptionSymbol` helper for OSI construction |
| `options.py`  | `bull_call_vertical_open/_close` (and bear/put variants) — native spread builders using `ComplexOrderStrategyType.VERTICAL`. Verticals only; no native "buy-write" spread. |
| `common.py`   | `one_cancels_other(o1, o2)` → wraps children as `OrderStrategyType.OCO`; `first_triggers_second(first, second)` → wraps children as `OrderStrategyType.TRIGGER` |
| `generic.py`  | `OrderBuilder` — the escape hatch for any shape schwab-py doesn't pre-bake |

**Key finding:** schwab-py has **no pre-built buy-write constructor**.
Covered calls must be assembled from the equity + option primitives.

---

## Architecture decision: TRIGGER chain

Three options evaluated:

1. **Sequential** — place equity BUY, poll for fill, then place option
   STO. Rejected: leg risk during the fill gap. If the stock rips, the
   call premium blows out before we sell.
2. **TRIGGER chain** *(chosen)* — equity BUY is the parent; on fill it
   atomically triggers the option STO child via Schwab's TRIGGER
   strategy. Native, server-side, no leg risk.
3. **Complex single-order net-debit buy-write** — would require building
   both legs in one `OrderBuilder` with `OrderType.NET_DEBIT` and a
   custom complex strategy. schwab-py doesn't expose a buy-write
   complex type out of the box. Revisit later for verticals/condors
   where `VERTICAL` is pre-baked.

**TRIGGER chain shape:**

```python
from schwab.orders import equities as eq, options as op
from schwab.orders.common import first_triggers_second

parent = (
    eq.equity_buy_limit(stock_symbol, qty, f"{stock_limit:.2f}")
    .set_duration(Duration.DAY)
    .set_session(Session.NORMAL)
)
child = (
    op.option_sell_to_open_limit(
        option_osi, contracts, f"{call_limit:.2f}"
    )
    .set_duration(Duration.GOOD_TILL_CANCEL)  # call limit outlives the equity fill day
    .set_session(Session.NORMAL)
)
order = first_triggers_second(parent, child).build()
resp = client.place_order(account_hash, order)
```

Quantities: `qty = contracts * 100` on the equity leg. Enforce
`contracts >= 1` and never fraction shares.

---

## Implementation checklist

### 1. `schwab_client.py` — new `place_buy_write`

```python
def place_buy_write(
    self,
    account_hash: str,
    stock_symbol: str,
    option_osi: str,
    contracts: int,
    stock_limit: float | None = None,  # None → buffered from quote
    call_limit: float | None = None,   # None → buffered from chain mid
    duration_child: str = "GOOD_TILL_CANCEL",
) -> dict
```

- Reuses `_compute_limit_price` for the equity leg when
  `stock_limit is None`.
- Call-leg price resolution: the caller (screener) already has
  `call_bid` / `call_ask` on the opportunity — pass `call_limit` in
  explicitly. Don't re-fetch the chain inside the client.
- Returns `{status: ok, parent_order_id, child_order_id?, stock_symbol,
  option_symbol, contracts, stock_limit, call_limit}` or
  `{status: error, error}`.
- Child order id may not be in the parent response; Schwab returns a
  single Location header for the parent. Leave `child_order_id` as
  `None` on first implementation; order tracker reads it from the
  child-orders array on the next poll.
- Gate everything behind the existing
  `_account_limiter._throttle` + error-wrap pattern in `place_order`.

### 2. `CoveredCallScreener.execute()`

Replace the no-op with:

```python
def execute(self, opportunity: dict) -> dict | None:
    if not self._should_execute(opportunity):
        return {"status": "dry_run", "opportunity": opportunity}

    # Risk checks — reuse existing path
    account = self._account
    qty = 100  # one contract = 100 shares; size up via config later
    allowed, reason = self.risk.can_buy(
        symbol=opportunity["symbol"],
        quantity=qty,
        price=opportunity["price"],
        account=account,
    )
    if not allowed:
        logger.warning("[covered_call_screener] risk veto: %s", reason)
        return {"status": "risk_veto", "reason": reason}

    result = self.client.place_buy_write(
        account_hash=account.account_hash,
        stock_symbol=opportunity["symbol"],
        option_osi=opportunity["call_symbol"],
        contracts=1,
        call_limit=opportunity["call_bid"],  # conservative: sell at bid
    )
    return result
```

**Sizing loop (v2, not this session):** allow N contracts per
opportunity by dividing `available_capital / (100 * spot)`. First
pass: 1 contract only.

### 3. Per-strategy live flag + config

Add to `config.py`:

```python
LIVE_COVERED_CALL_SCREENER: bool = False
```

And to `_STRATEGY_LIVE_FLAGS`:

```python
"covered_call_screener": "LIVE_COVERED_CALL_SCREENER",
```

### 4. Telegram approval gate

The existing `TELEGRAM_REQUIRE_APPROVAL=true` path shows Approve/Reject
buttons for single orders. Multi-leg needs a multi-line approval
message:

```
Buy-Write proposal · KO
  BUY 100 KO @ LIMIT $60.02
  STO  1 KO 2026-05-20 $63 CALL @ LIMIT $1.10
  Net debit: $5892.00 | Max profit: $408 (if called)
  Annualized if-called: 72% | Div in hold: $0.48
  [Approve] [Reject]
```

Hook into the approval layer in `telegram_bot.py` (or wherever the
existing gate lives — grep `require_approval`). The opportunity dict
already has everything needed to render this message.

### 5. Order tracker

`order_tracker.py` currently watches single orders. For a TRIGGER
parent:
- Parent status transitions: PENDING → WORKING → FILLED → (triggers
  child)
- Child order appears in the `orderLegCollection` / `childOrderStrategies`
  on subsequent polls of the account's orders list.
- Need to: (a) detect the child via `childOrderStrategies[0].orderId`
  after parent fill, (b) track its own fill state, (c) emit separate
  fill events for the equity leg and the option leg.

First implementation can be naive: treat the parent and child
independently. Poll all working orders as today; the child appears as
a new WORKING order after the parent fills. No explicit parent→child
linking is required for P&L tracking in v1.

### 6. Tests

`tests/test_buy_write.py` (new):

- Monkeypatch `schwab.orders.equities` + `schwab.orders.options` to
  capture the builder calls and assert:
  - Parent is `equity_buy_limit` with correct qty and price
  - Child is `option_sell_to_open_limit` with correct OSI, contracts,
    price
  - `first_triggers_second(parent, child).build()` called once
  - Durations/sessions match the signature
- `place_buy_write` error paths: empty account hash, contracts ≤ 0,
  missing quotes for auto-pricing, schwab-py import failure
- `CoveredCallScreener.execute()` flow:
  - DRY_RUN → returns `dry_run` dict, no client call
  - LIVE=false per-strategy flag → same as DRY_RUN
  - risk veto → returns `risk_veto`, no client call
  - happy path → calls `place_buy_write` exactly once with the
    expected kwargs

Existing 658-test suite should still pass.

---

## Risk integration

| Gate | Behaviour |
|------|-----------|
| Event blackout (earnings) | Already fires on any BUY via `trading_rules.check_order`. Covered-call entries will hit this naturally. **Decide:** do we want to *allow* the entry pre-earnings for IV-crush premium capture? Currently it warns; flipping to `block` would hurt the screener's main alpha source. Default: keep `warn`. |
| Dividend blackout | Off by default. Leave off — the screener *wants* dividends in the hold window. |
| Wash sale | Existing warning-only wash-sale rule fires on the equity BUY. Acceptable. |
| PDT | Not a day trade unless we close same day. Covered calls held to expiry are fine on margin<25k. |
| Position cap | `MAX_POSITION_VALUE` / `MAX_POSITION_PCT` must accommodate `100 * spot` per contract. A $300 stock = $30k per contract — too big for a $50k account under the default 10% position cap. Screener should filter opportunities by `spot <= max_position_value / 100` before ranking. Add a config knob: `COVERED_CALL_MAX_SPOT`. |

---

## Open questions to resolve during implementation

1. **Does Schwab honour TRIGGER across asset classes?** The docs say
   TRIGGER supports mixed parent/child types, but verify with a
   DRY_RUN=false test in paper mode before live. If it rejects the
   chain, fall back to sequential with a 5-second poll loop.
2. **OSI symbol format.** `OptionContract.symbol` returned by
   `get_option_chain()` already looks like `"KO   260520C00063000"` in
   Schwab's format. Confirm schwab-py's `option_sell_to_open_limit`
   accepts that exact string. If not, reformat via
   `OptionSymbol(underlying, exp, side, strike).build()`.
3. **Cash-secured vs margin covered call.** With a cash account +
   TRIGGER, does Schwab allow the child STO to submit before the
   equity is *settled*? Day-of-fill the shares are long but unsettled;
   some brokers require T+1 settlement before allowing covered writes.
   Test in paper first. Workaround if blocked: delay the STO by one
   trading day via `first_triggers_second` + `Duration.GOOD_TILL_CANCEL`
   and let Schwab's server handle the queue, OR split into two
   sessions.
4. **Partial equity fill.** If the parent partial-fills (e.g. 67 out of
   100 shares), does the child trigger immediately with the full
   contract size, exposing an uncovered short call? Schwab's TRIGGER
   semantics for partials need to be confirmed — worst case, require
   `OrderType.FOK` or single-lot parents.
5. **Approval gate latency.** The screener runs on a schedule; if
   approval takes 5+ minutes the chain mid will have moved. Attach an
   `approval_timeout` and re-quote on approval before submitting.
6. **Order tracker parent/child join.** Confirm the Schwab
   `get_orders` response nests children under
   `childOrderStrategies[].orderId` so `order_tracker` can tag both
   legs as "covered_call_screener".

---

## Rollout sequence

1. **Dry-run (zero-risk).** Build everything with `DRY_RUN=true`. The
   `execute()` path returns `dry_run` with a rendered order payload.
   Verify the builder outputs in tests + one manual log inspection.
2. **Paper account.** Flip `DRY_RUN=false` on a paper Schwab account
   only. Execute 1 contract on a liquid name (KO, XOM). Verify:
   - Parent equity order fills cleanly
   - Child STO triggers and reaches WORKING
   - Order tracker logs both fills
   - `strategy_pnl.json` picks up the combined cost basis
3. **Live, 1 contract.** Turn on
   `LIVE_COVERED_CALL_SCREENER=true` + `TELEGRAM_REQUIRE_APPROVAL=true`.
   Cap `COVERED_CALL_TOP_N=1` for the first few entries so only the
   single best opportunity per scan tries to execute. Watch for a full
   week before lifting the cap.
4. **Scale up.** Raise `TOP_N`, add contract sizing, then open up to
   the full universe.

**Kill switches at every step:**
- `LIVE_COVERED_CALL_SCREENER=false` in `.env` → scan-only
- `DRY_RUN=true` globally → nothing leaves the process
- `/kill` in Telegram → risk manager kill switch stops everything
- Child-order hygiene: on any parent reject/cancel, verify no child
  orphan was submitted. Add a post-execute assertion in `execute()`.

---

## Deliverables for the next session

**Session-close target:** paper account executes one buy-write
successfully, both legs fill, order tracker logs them, suite green.

1. `schwab_client.place_buy_write()` + unit tests
2. `CoveredCallScreener.execute()` wired with risk + live-flag gates
3. `config.LIVE_COVERED_CALL_SCREENER` + `_STRATEGY_LIVE_FLAGS` entry
4. `COVERED_CALL_MAX_SPOT` knob + screener filter
5. Telegram approval message multi-leg renderer
6. `tests/test_buy_write.py` + `test_covered_call.py::TestExecute`
7. Paper-mode smoke test run, captured in a follow-up note

**Not in scope:**
- Verticals / condors / butterflies (port once buy-write works)
- Rolling logic (close expiring call + open next month)
- Assignment handling (Schwab auto-exercises; P&L reconciliation
  against `trade_history.jsonl` is a separate session)
- Cross-account execution routing

---

## Follow-up: verticals + iron condor (2026-04-22)

Extending the multi-leg plumbing to cover 2-leg verticals (all 4 spread
types × open/close) and 4-leg iron condors. Client-level primitives
only this pass — screeners, live flags, Telegram renderers are deferred
to a later session, matching the buy-write rollout.

### Shipped

- **`SchwabClient.place_vertical(spread_type, action, long_osi,
  short_osi, quantity, net_price, ...)`** — generic dispatcher over all
  8 schwab-py native vertical constructors
  (`bull_call / bear_call / bull_put / bear_put` × `open / close`).
  Caller always passes `(long_osi, short_osi)`; the method swaps leg
  ordering internally for bear builders (which expect
  `(short, long)`). OrderType (NET_DEBIT / NET_CREDIT) and
  `ComplexOrderStrategyType.VERTICAL` are set by the builder.
- **`SchwabClient.place_iron_condor(action, long_put_osi, short_put_osi,
  short_call_osi, long_call_osi, quantity, net_price, ...)`** — 4-leg
  hand-built `OrderBuilder` (schwab-py has no iron-condor helper) with
  `ComplexOrderStrategyType.IRON_CONDOR`. Open → NET_CREDIT with
  BTO/STO legs; close → NET_DEBIT with STC/BTC legs. Strike ordering
  (`long_put < short_put < short_call < long_call`) is the caller's
  responsibility — not validated here.
- **38 new tests** across `tests/test_verticals.py` (22, parametrized
  over all 8 combos) and `tests/test_iron_condor.py` (16, open + close
  paths + per-field validation). Suite: 684 → 733.

### Deferred to follow-up sessions

1. **Vertical screener strategies** — a `BullPutSpreadScreener` is the
   natural income analog to `CoveredCallScreener` (sell OTM put spread
   for credit). Also useful: `BearCallSpreadScreener` for overbought
   names. Same scan / rank / execute shape as the covered-call path.
2. **Iron condor screener** — scan high-IV names, pick short strikes
   at ~±0.16 delta (~1σ), long wings at a configurable width.
3. **Per-strategy live flags** — `LIVE_BULL_PUT_SPREAD`,
   `LIVE_BEAR_CALL_SPREAD`, `LIVE_IRON_CONDOR`, etc., registered in
   `config._STRATEGY_LIVE_FLAGS` (mirrors `LIVE_COVERED_CALL_SCREENER`).
4. **Telegram approval renderer** — multi-line proposals for 2-leg
   verticals and 4-leg iron condors with net credit/debit, max P/L,
   breakevens. Hook into the existing `trade_type`-dispatched
   approval gate added for `buy_write`.
5. **Margin / BP checks** — credit spreads require `short_strike -
   long_strike` * 100 * contracts in buying power reservation. Iron
   condors: margin is the larger of the two spread widths (Schwab
   doesn't double-margin). Risk manager extension needed before live.
6. **Paper-mode smoke test** — confirm Schwab accepts the hand-built
   IRON_CONDOR complex type (it's a rarer strategy than VERTICAL; the
   builder dict may need tweaks if Schwab rejects).
7. **Butterflies** — separate headspace (pin-risk profile). Own
   session.
