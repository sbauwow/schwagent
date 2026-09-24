"""Tax-aware rebalancing of one target allocation across every account. Advisory only.

The household (all mapped accounts together) should hold ``REBALANCE_TARGETS``
within ``REBALANCE_BAND``. Which account holds what is chosen by a linear program:

* **Cash can't cross accounts.** Each account's buys are funded by its own cash
  and sells — you can't move money between an IRA and a taxable account.
* **Selling costs tax only in taxable accounts,** and there each lot is its own
  segment priced at ``gain / value * rate`` (short- or long-term), so the solver
  sells cheapest-to-sell lots first and prefers rebalancing inside IRAs. Loss
  lots cost nothing to sell (harvesting proper is ``tax_harvest``).
* **Asset location:** a tax-inefficient fund (``TAX_INEFFICIENT``) held in a
  taxable or Roth account carries a yearly drag relative to the best account
  available, so new money for bonds and REITs flows to traditional IRAs first.
* **No wash sales:** if the plan would sell a symbol at a loss and buy it in any
  account, it's re-solved with those loss lots held.

Holdings outside the target list are left alone and reported.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np

from schwabagent.config import Config
from schwabagent.household import Account, Household
from schwabagent.tax_lots import Lot

logger = logging.getLogger(__name__)

TURNOVER_COST = 0.0005  # per dollar traded; stops the solver churning for nothing
_MAX_WASH_PASSES = 5


@dataclass
class Trade:
    account: str
    tax_type: str
    symbol: str
    side: str  # "BUY" | "SELL"
    shares: int
    price: float
    lots: list[tuple[Lot, float]] = field(default_factory=list)  # (lot, shares) for sells
    short_term_gain: float = 0.0
    long_term_gain: float = 0.0

    @property
    def usd(self) -> float:
        return self.shares * self.price


@dataclass
class RebalancePlan:
    targets: dict[str, float]
    managed_value: float
    before: dict[str, float]  # symbol -> household weight ("CASH" included)
    after: dict[str, float]
    trades: list[Trade]
    estimated_tax: float
    unmanaged: dict[str, float]  # symbol -> dollars left untouched
    warnings: list[str] = field(default_factory=list)


def _segments(account: Account, symbol: str, hh: Household, config: Config) -> list[tuple[float, float, Lot | None]]:
    """(dollars, tax cost per dollar sold, lot) for what ``account`` holds of ``symbol``."""
    price = hh.prices[symbol]
    holding = account.holdings.get(symbol)
    if holding is None:
        return []
    if account.tax_type != "taxable":
        return [(holding.market_value, 0.0, None)]
    segs = []
    for lot in hh.lots.get((account.number, symbol), []):
        rate = config.TAX_RATE_LONG if lot.is_long_term(hh.asof) else config.TAX_RATE_SHORT
        gain_frac = (price - lot.cost_per_share) / price
        segs.append((lot.qty * price, max(gain_frac, 0.0) * rate, lot))
    return sorted(segs, key=lambda s: s[1])


def _location_drag(tax_type: str, symbol: str, inefficient: set[str], config: Config) -> float:
    if symbol not in inefficient:
        return 0.0
    return {"taxable": config.LOCATION_DRAG_TAXABLE, "roth": config.LOCATION_DRAG_ROTH}.get(tax_type, 0.0)


def plan_rebalance(hh: Household, targets: dict[str, float], config: Config) -> RebalancePlan:
    import cvxpy as cp

    symbols = [s.upper() for s in targets]
    weights = np.array([targets[s] for s in targets])
    missing = [s for s in symbols if s not in hh.prices]
    if missing:
        raise ValueError(f"no price for target symbol(s): {', '.join(missing)}")

    accounts = [a for a in hh.accounts if a.tax_type is not None and a.total_value > 0]
    warnings = [
        f"...{a.number} (${a.total_value:,.0f}) has no ACCOUNT_TAX_TYPES entry — left out"
        for a in hh.accounts if a.tax_type is None and a.total_value > 0
    ]
    if not accounts:
        raise ValueError("no accounts mapped in ACCOUNT_TAX_TYPES")
    inefficient = {s.upper() for s in config.TAX_INEFFICIENT.split(",") if s.strip()}

    current = np.array([[a.holdings[s].market_value if s in a.holdings else 0.0 for s in symbols]
                        for a in accounts])
    sweep = {s.upper() for s in config.CASH_EQUIVALENTS.split(",") if s.strip()} - set(symbols)
    sweep_value = np.array([sum(h.market_value for s, h in a.holdings.items() if s in sweep) for a in accounts])
    cash = np.array([max(a.cash + sweep_value[i] - config.REBALANCE_CASH_BUFFER_USD, 0.0)
                     for i, a in enumerate(accounts)])
    investable = current.sum(axis=1) + cash
    managed = float(investable.sum())
    segs = {(i, j): _segments(a, s, hh, config)
            for i, a in enumerate(accounts) for j, s in enumerate(symbols)}
    drag = np.array([[_location_drag(a.tax_type, s, inefficient, config) for s in symbols]
                     for a in accounts])
    # Only the gap to the best account matters: with nowhere better to hold a
    # fund, its drag must not bias how much of it the household holds.
    drag = drag - drag.min(axis=0, keepdims=True)

    # Symbols whose loss lots must not be sold: bought recently anywhere (lookback)
    # or bought by the plan itself (lookahead). Grows until the plan is wash-free.
    no_loss_sale = {
        s for s in symbols
        if any(lot.acquired is not None and (hh.asof - lot.acquired).days <= config.WASH_SALE_DAYS
               for (acct, sym), lots in hh.lots.items() if sym == s for lot in lots)
    }
    for _ in range(_MAX_WASH_PASSES):
        z_vars, cost_terms = {}, []
        constraints = []
        buy = cp.Variable((len(accounts), len(symbols)), nonneg=True)
        sold = 0
        for (i, j), seg_list in segs.items():
            if not seg_list:
                continue
            z = cp.Variable(len(seg_list), nonneg=True)
            caps = np.array([v for v, _, _ in seg_list])
            for k, (_, _, lot) in enumerate(seg_list):
                if lot is not None and symbols[j] in no_loss_sale and lot.unrealized(hh.prices[symbols[j]]) < 0:
                    caps[k] = 0.0
            constraints.append(z <= caps)
            z_vars[(i, j)] = z
            cost_terms.append(np.array([c for _, c, _ in seg_list]) @ z + TURNOVER_COST * cp.sum(z))
            one_hot = np.zeros(current.shape)
            one_hot[i, j] = 1.0
            sold = sold + cp.sum(z) * one_hot
        held = current - sold + buy
        total = cp.sum(held, axis=0)
        # Cash beyond the target's implied cash is penalized above any location
        # drag, so parking money never beats holding the fund somewhere.
        target_cash = max(1 - weights.sum(), 0.0) * managed
        excess_cash = cp.Variable(nonneg=True)
        constraints += [
            cp.sum(held, axis=1) <= investable,
            total >= (weights - config.REBALANCE_BAND) * managed,
            total <= (weights + config.REBALANCE_BAND) * managed,
            excess_cash >= managed - cp.sum(total) - target_cash,
            excess_cash <= config.REBALANCE_BAND * managed,
        ]
        idle_cost = float(drag.max(initial=0.0)) + 2 * TURNOVER_COST
        objective = cp.Minimize(sum(cost_terms) + TURNOVER_COST * cp.sum(buy)
                                + cp.sum(cp.multiply(drag, held)) + idle_cost * excess_cash)
        problem = cp.Problem(objective, constraints)
        problem.solve()
        if problem.status not in ("optimal", "optimal_inaccurate"):
            raise RuntimeError(f"rebalance LP {problem.status}")

        buys = buy.value
        washed = {
            symbols[j] for (i, j), z in z_vars.items()
            if buys[:, j].sum() > 1.0 and any(
                z.value[k] > 1.0 and lot is not None and lot.unrealized(hh.prices[symbols[j]]) < 0
                for k, (_, _, lot) in enumerate(segs[(i, j)]))
        }
        if not washed - no_loss_sale:
            break
        no_loss_sale |= washed

    trades = _to_trades(accounts, symbols, segs, z_vars, buys, hh, config)
    lp_cash = investable - held.value.sum(axis=1)  # cash the LP meant to keep
    _top_up(trades, accounts, symbols, cash - lp_cash, buys, weights, managed, current, hh, config)
    after_value = current.copy()
    for t in trades:
        i = next(i for i, a in enumerate(accounts) if a.number == t.account)
        after_value[i, symbols.index(t.symbol)] += t.usd if t.side == "BUY" else -t.usd

    _fund_from_sweep(trades, accounts, sweep, hh)
    trades.sort(key=lambda t: (t.account, t.side != "SELL", t.symbol))

    loss_held = {sym for (acct, sym), lots in hh.lots.items()
                 if hh.account(acct).tax_type == "taxable" and sym in hh.prices
                 and any(lot.unrealized(hh.prices[sym]) < 0 for lot in lots)}
    for sym in sorted({t.symbol for t in trades if t.side == "BUY"} & loss_held):
        warnings.append(f"buying {sym} blocks harvesting its taxable loss lots until "
                        f"{hh.asof + timedelta(days=config.WASH_SALE_DAYS + 1)} "
                        "— harvest first (./run.sh harvest) if you want that loss")

    unmanaged: dict[str, float] = {}
    for a in accounts:
        for sym, h in a.holdings.items():
            if sym not in symbols and sym not in sweep:
                unmanaged[sym] = unmanaged.get(sym, 0.0) + h.market_value

    def _weights(values: np.ndarray) -> dict[str, float]:
        out = {s: float(values[:, j].sum()) / managed for j, s in enumerate(symbols)}
        out["CASH"] = 1 - sum(out.values())
        return out

    return RebalancePlan(
        targets={s: float(w) for s, w in zip(symbols, weights)},
        managed_value=managed,
        before=_weights(current),
        after=_weights(after_value),
        trades=trades,
        estimated_tax=sum(t.short_term_gain * config.TAX_RATE_SHORT + t.long_term_gain * config.TAX_RATE_LONG
                          for t in trades),
        unmanaged=unmanaged,
        warnings=warnings,
    )


def _to_trades(accounts, symbols, segs, z_vars, buys, hh, config) -> list[Trade]:
    """Round the LP's dollar moves to whole shares; name the lots behind each sell."""
    trades: list[Trade] = []
    for i, account in enumerate(accounts):
        for j, symbol in enumerate(symbols):
            price = hh.prices[symbol]
            sold_usd = float(z_vars[(i, j)].value.sum()) if (i, j) in z_vars else 0.0
            net = float(buys[i, j]) - sold_usd
            if abs(net) < config.REBALANCE_MIN_TRADE_USD:
                continue
            if net > 0:
                shares = math.floor(net / price + 1e-6)
                if shares * price >= config.REBALANCE_MIN_TRADE_USD:
                    trades.append(Trade(account.number, account.tax_type, symbol, "BUY", shares, price))
                continue
            held = account.holdings[symbol].qty
            shares = min(round(-net / price), math.floor(held + 1e-6))
            if shares <= 0 or shares * price < config.REBALANCE_MIN_TRADE_USD:
                continue
            trade = Trade(account.number, account.tax_type, symbol, "SELL", shares, price)
            if account.tax_type == "taxable":
                remaining = float(shares)
                for _, _, lot in segs[(i, j)]:  # cheapest tax cost first, as the LP chose
                    if remaining <= 1e-9:
                        break
                    take = min(lot.qty, remaining)
                    remaining -= take
                    trade.lots.append((lot, take))
                    gain = take * (price - lot.cost_per_share)
                    if lot.is_long_term(hh.asof):
                        trade.long_term_gain += gain
                    else:
                        trade.short_term_gain += gain
            trades.append(trade)
    trades.sort(key=lambda t: (t.account, t.side != "SELL", t.symbol))
    return trades


def _fund_from_sweep(trades: list[Trade], accounts, sweep: set[str], hh: Household) -> None:
    """Sell money-market shares where an account's buys exceed its settled cash."""
    for account in accounts:
        mine = [t for t in trades if t.account == account.number]
        need = sum(t.usd for t in mine if t.side == "BUY") - sum(t.usd for t in mine if t.side == "SELL") - account.cash
        for sym in sorted(s for s in account.holdings if s in sweep):
            if need <= 0:
                break
            holding = account.holdings[sym]
            price = holding.market_value / holding.qty
            shares = min(math.ceil(need / price), math.floor(holding.qty + 1e-6))
            if shares > 0:
                trades.append(Trade(account.number, account.tax_type, sym, "SELL", shares, price))
                need -= shares * price


def _top_up(trades, accounts, symbols, spendable, buys, weights, managed, current, hh, config) -> None:
    """Spend cash that whole-share rounding left behind in each account.

    Adds shares of the symbols the LP was already buying there, most
    underweight first, without pushing any symbol past its upper band.
    """
    totals = current.sum(axis=0)
    for t in trades:
        totals[symbols.index(t.symbol)] += t.usd if t.side == "BUY" else -t.usd
    ceiling = (weights + config.REBALANCE_BAND) * managed
    for i, account in enumerate(accounts):
        mine = [t for t in trades if t.account == account.number]
        spare = spendable[i] + sum(t.usd for t in mine if t.side == "SELL") - sum(t.usd for t in mine if t.side == "BUY")
        candidates = [j for j in range(len(symbols)) if buys[i, j] > 1.0]
        while candidates:
            j = min(candidates, key=lambda j: totals[j] / managed - weights[j])
            price = hh.prices[symbols[j]]
            if price > spare + 1e-6 or totals[j] + price > ceiling[j] + 1e-6:
                candidates.remove(j)
                continue
            trade = next((t for t in mine if t.symbol == symbols[j] and t.side == "BUY"), None)
            if trade is None:
                trade = Trade(account.number, account.tax_type, symbols[j], "BUY", 0, price)
                trades.append(trade)
                mine.append(trade)
            trade.shares += 1
            spare -= price
            totals[j] += price
    trades[:] = [t for t in trades if t.side == "SELL" or t.usd >= config.REBALANCE_MIN_TRADE_USD]
    trades.sort(key=lambda t: (t.account, t.side != "SELL", t.symbol))


def targets_from_optimizer(client, symbols: list[str], method: str, days: int = 756) -> dict[str, float]:
    """Target weights from ``portfolio_optimizer`` over ``days`` of daily closes."""
    from schwabagent.portfolio_optimizer import optimize_portfolio

    prices = {s: client.get_ohlcv(s, days=days) for s in symbols}
    result = optimize_portfolio(prices, method=method)
    return {s: w for s, w in result.weights.items() if w > 1e-4}

