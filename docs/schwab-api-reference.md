# Schwab API Reference — Data Returned by Endpoint

Captured from live API calls on 2026-04-04 against account 53259297 (Cash HSA Brokerage).

---

## Dual API Architecture

Schwab exposes two separate API products, each requiring its own app registration, API key, and OAuth token:

| API | Base URL | Purpose |
|-----|----------|---------|
| **Trader API** | `https://api.schwabapi.com/trader/v1` | Accounts, positions, balances, orders, transactions |
| **Market Data API** | `https://api.schwabapi.com/marketdata/v1` | Quotes, price history, options chains, movers, instruments |

A single app registration *can* have both products enabled, but Schwab allows (and the rebalancer uses) separate apps for each. The callback URL registered on developer.schwab.com must match exactly during OAuth enrollment.

---

## Trader API Endpoints

### GET /accounts (with positions)

Returns an array of all accounts linked to the authenticated app.

```
GET /trader/v1/accounts?fields=positions
```

#### Response: `securitiesAccount`

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `type` | string | `"CASH"` | `"CASH"` or `"MARGIN"` — determines PDT rule applicability |
| `accountNumber` | string | `"53259297"` | Full account number |
| `roundTrips` | int | `0` | Schwab-tracked day trades in rolling 5 business days |
| `isDayTrader` | bool | `false` | Schwab PDT flag — set when round trips exceed limit on margin |
| `isClosingOnlyRestricted` | bool | `false` | If true, account can only close positions (no new buys) |
| `pfcbFlag` | bool | `false` | Penny stock / free-riding flag |

#### Response: `initialBalances`

Snapshot of account balances at the start of the trading day.

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `accruedInterest` | float | `0.0` | |
| `cashAvailableForTrading` | float | `8812.08` | |
| `cashAvailableForWithdrawal` | float | `8812.08` | |
| `cashBalance` | float | `8812.08` | |
| `bondValue` | float | `0.0` | |
| `cashReceipts` | float | `0.0` | |
| `liquidationValue` | float | `8812.08` | Total account value if everything were sold |
| `longOptionMarketValue` | float | `0.0` | |
| `longStockValue` | float | `0.0` | |
| `moneyMarketFund` | float | `0.0` | |
| `mutualFundValue` | float | `8812.08` | Includes sweep fund |
| `shortOptionMarketValue` | float | `0.0` | |
| `shortStockValue` | float | `0.0` | |
| `isInCall` | bool | `false` | Margin call status |
| `unsettledCash` | float | `0.0` | Cash from trades not yet settled (T+1) |
| `cashDebitCallValue` | float | `0.0` | |
| `pendingDeposits` | float | `0.0` | |
| `accountValue` | float | `8812.08` | |

#### Response: `currentBalances`

Live balances reflecting intraday activity.

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `accruedInterest` | float | `0.0` | |
| `cashBalance` | float | `8812.08` | |
| `cashReceipts` | float | `0.0` | |
| `longOptionMarketValue` | float | `0.0` | |
| `liquidationValue` | float | `8812.08` | **Primary value we use for portfolio total** |
| `longMarketValue` | float | `0.0` | Total long equity market value |
| `moneyMarketFund` | float | `0.0` | |
| `savings` | float | `0.0` | |
| `shortMarketValue` | float | `0.0` | |
| `pendingDeposits` | float | `0.0` | |
| `mutualFundValue` | float | `0.0` | |
| `bondValue` | float | `0.0` | |
| `shortOptionMarketValue` | float | `0.0` | |
| `cashAvailableForTrading` | float | `8812.08` | **What we use for available cash** |
| `cashAvailableForWithdrawal` | float | `8812.08` | |
| `cashCall` | float | `0.0` | |
| `longNonMarginableMarketValue` | float | `8812.08` | |
| `totalCash` | float | `8812.08` | |
| `cashDebitCallValue` | float | `0.0` | |
| `unsettledCash` | float | `0.0` | **Important for cash accounts — funds pending T+1 settlement** |

Note: margin accounts return additional fields including `buyingPower`, `marginBalance`, `maintenanceCall`, `equity`, `dayTradingBuyingPower`, etc.

#### Response: `projectedBalances`

End-of-day projected values.

| Field | Type | Example |
|-------|------|---------|
| `cashAvailableForTrading` | float | `8812.08` |
| `cashAvailableForWithdrawal` | float | `8812.08` |

#### Response: `positions[]` (when requested with `fields=positions`)

Array of current holdings. Empty when no positions held.

| Field | Type | Notes |
|-------|------|-------|
| `instrument.symbol` | string | Ticker symbol |
| `instrument.cusip` | string | CUSIP identifier |
| `instrument.assetType` | string | `"EQUITY"`, `"ETF"`, `"OPTION"`, `"MUTUAL_FUND"` etc. |
| `longQuantity` | float | Shares held long |
| `shortQuantity` | float | Shares held short |
| `marketValue` | float | Current market value |
| `averagePrice` | float | Average cost basis per share |
| `currentDayProfitLoss` | float | Today's unrealized P&L |
| `currentDayProfitLossPercentage` | float | Today's unrealized P&L % |
| `unrealizedProfitLoss` | float | Total unrealized P&L (not available on all account types) |
| `settledLongQuantity` | float | Settled shares (post T+1) |
| `settledShortQuantity` | float | |
| `agedQuantity` | float | |

#### Response: `aggregatedBalance`

| Field | Type | Example |
|-------|------|---------|
| `currentLiquidationValue` | float | `8812.08` |
| `liquidationValue` | float | `8812.08` |

---

### GET /accounts/accountNumbers

Returns account number to hash mapping.

```
GET /trader/v1/accounts/accountNumbers
```

| Field | Type | Example |
|-------|------|---------|
| `accountNumber` | string | `"53259297"` |
| `hashValue` | string | `"A4B723D2321FD0A9E47D8CAF968096652B1D4859F5B0DAEE3D340A10E99909F7"` |

The `hashValue` is required for all per-account API calls (orders, transactions, etc.).

---

### GET /accounts/{hash}/orders

Returns orders for the specified time range.

```
GET /trader/v1/accounts/{hash}/orders?fromEnteredDatetime=...&toEnteredDatetime=...
```

Returns an array of order objects. Empty array `[]` when no orders in range.

Order objects contain fields including:
- `orderId`, `status` (`FILLED`, `WORKING`, `CANCELED`, etc.)
- `orderType` (`MARKET`, `LIMIT`, etc.)
- `session` (`NORMAL`, `AM`, `PM`, `SEAMLESS`)
- `price`, `quantity`, `filledQuantity`
- `orderLegCollection[]` with `instrument`, `instruction` (`BUY`, `SELL`), `quantity`
- `orderActivityCollection[]` with execution details

---

### GET /userPreference

Returns user-level account metadata and streaming configuration.

```
GET /trader/v1/userPreference
```

#### Response: `accounts[]`

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `accountNumber` | string | `"53259297"` | |
| `primaryAccount` | bool | `false` | |
| `type` | string | `"BROKERAGE"` | Account product type (not cash/margin) |
| `nickName` | string | `"HSA Brokerage"` | User-assigned nickname |
| `displayAcctId` | string | `"...297"` | Masked display ID |
| `autoPositionEffect` | bool | `false` | |
| `accountColor` | string | `"Green"` | UI color tag |
| `lotSelectionMethod` | string | `"FIFO"` | Tax lot method: FIFO, LIFO, HighCost, LowCost, etc. |
| `hasFuturesAccount` | bool | `false` | |
| `hasForexAccount` | bool | `false` | |

#### Response: `streamerInfo[]`

WebSocket streaming connection details.

| Field | Type | Example |
|-------|------|---------|
| `streamerSocketUrl` | string | `"wss://streamer-api.schwab.com/ws"` |
| `schwabClientCustomerId` | string | `"ac572ee7..."` |
| `schwabClientCorrelId` | string | `"8e9fc397-..."` |
| `schwabClientChannel` | string | `"N9"` |
| `schwabClientFunctionId` | string | `"APIAPP"` |

#### Response: `offers[]`

| Field | Type | Example |
|-------|------|---------|
| `level2Permissions` | bool | `true` |
| `mktDataPermission` | string | `"NP"` |

---

## Market Data API Endpoints

### GET /quotes

Real-time quotes for one or more symbols.

```
GET /marketdata/v1/quotes?symbols=SPY,AAPL
```

Each symbol returns a nested object with these sections:

#### Top-level

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `assetMainType` | string | `"EQUITY"` | |
| `assetSubType` | string | `"ETF"` or `"COE"` | ETF vs common equity |
| `quoteType` | string | `"NBBO"` | National Best Bid/Offer |
| `realtime` | bool | `true` | |
| `ssid` | int | `1281357639` | Schwab security ID |
| `symbol` | string | `"SPY"` | |

#### `quote` (current trading data)

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `52WeekHigh` | float | `697.84` | |
| `52WeekLow` | float | `481.80` | |
| `askPrice` | float | `655.95` | |
| `askSize` | int | `80` | In round lots |
| `askTime` | long | `1775174399228` | Epoch ms |
| `askMICId` | string | `"ARCX"` | Market center |
| `bidPrice` | float | `655.87` | |
| `bidSize` | int | `120` | |
| `bidTime` | long | `1775174399914` | |
| `bidMICId` | string | `"ARCX"` | |
| `closePrice` | float | `655.24` | Previous close |
| `highPrice` | float | `658.20` | Today's high |
| `lowPrice` | float | `645.11` | Today's low |
| `openPrice` | float | `646.42` | Today's open |
| `lastPrice` | float | `655.92` | Last trade price |
| `lastSize` | int | `220` | Last trade size |
| `lastMICId` | string | `"ARCX"` | |
| `mark` | float | `655.83` | Mark price |
| `markChange` | float | `0.59` | |
| `markPercentChange` | float | `0.09` | |
| `netChange` | float | `0.68` | Change from close |
| `netPercentChange` | float | `0.104` | |
| `postMarketChange` | float | `0.09` | Extended hours change |
| `postMarketPercentChange` | float | `0.014` | |
| `quoteTime` | long | `1775174399914` | |
| `tradeTime` | long | `1775174398360` | |
| `securityStatus` | string | `"Normal"` | or `"Closed"` |
| `totalVolume` | int | `68358713` | Today's total volume |

#### `fundamental` (per-symbol fundamentals)

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `avg10DaysVolume` | float | `114659923` | |
| `avg1YearVolume` | float | `71517269` | |
| `declarationDate` | string | `"2026-01-02T..."` | Last dividend declaration |
| `divAmount` | float | `7.188` | Annual dividend |
| `divExDate` | string | `"2026-03-20T..."` | Last ex-dividend date |
| `divFreq` | int | `4` | Quarterly |
| `divPayAmount` | float | `1.797` | Per-payment amount |
| `divPayDate` | string | `"2026-04-30T..."` | |
| `divYield` | float | `1.097` | Annual yield % |
| `eps` | float | `99.73` | Earnings per share (aggregated for ETFs) |
| `fundLeverageFactor` | float | `100.0` | ETF leverage (100 = 1x) |
| `lastEarningsDate` | string | `"2025-11-25T..."` | |
| `nextDivExDate` | string | `"2026-06-22T..."` | Upcoming ex-date |
| `nextDivPayDate` | string | `"2026-07-30T..."` | |
| `peRatio` | float | `6.576` | Price/Earnings |
| `sharesOutstanding` | long | `997132116` | |

#### `reference` (security metadata)

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `cusip` | string | `"78462F103"` | |
| `description` | string | `"State Street SPDR S&P 500 ETF Trust"` | Full name |
| `exchange` | string | `"P"` | Exchange code |
| `exchangeName` | string | `"NYSE Arca"` | |
| `isHardToBorrow` | bool | `false` | Short selling availability |
| `isShortable` | bool | `true` | |
| `htbRate` | float | `0.0` | Hard-to-borrow fee rate |

#### `regular` (regular session only)

| Field | Type | Example |
|-------|------|---------|
| `regularMarketLastPrice` | float | `655.83` |
| `regularMarketLastSize` | int | `952689` |
| `regularMarketNetChange` | float | `0.59` |
| `regularMarketPercentChange` | float | `0.09` |
| `regularMarketTradeTime` | long | `1775174400001` |

#### `extended` (pre/post market)

| Field | Type | Example |
|-------|------|---------|
| `lastPrice` | float | `648.55` |
| `lastSize` | int | `5` |
| `mark` | float | `0.0` |
| `totalVolume` | int | `0` |
| `tradeTime` | long | `1775116786000` |

---

### GET /pricehistory

OHLCV candle data.

```
GET /marketdata/v1/pricehistory?symbol=SPY&periodType=day&period=10&frequencyType=daily
```

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `symbol` | string | `"SPY"` | |
| `empty` | bool | `false` | True when no data returned |

#### `candles[]`

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `open` | float | `658.67` | |
| `high` | float | `660.89` | |
| `low` | float | `654.24` | |
| `close` | float | `656.82` | |
| `volume` | int | `90653787` | |
| `datetime` | long | `1774414800000` | Epoch milliseconds |

---

### GET /chains (options)

Options chain data.

```
GET /marketdata/v1/chains?symbol=SPY&strikeCount=2
```

#### Top-level

| Field | Type | Example |
|-------|------|---------|
| `symbol` | string | `"SPY"` |
| `status` | string | `"SUCCESS"` |
| `strategy` | string | `"SINGLE"` |
| `isDelayed` | bool | `false` |
| `isIndex` | bool | `false` |
| `interestRate` | float | `3.607` |
| `underlyingPrice` | float | `655.83` |
| `volatility` | float | `29.0` |
| `numberOfContracts` | int | `144` |

#### `callExpDateMap` / `putExpDateMap` → `{expDate}` → `{strike}` → contract[]

Each contract:

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `putCall` | string | `"CALL"` | |
| `symbol` | string | `"SPY   260406C00655000"` | OCC symbol |
| `description` | string | `"SPY 04/06/2026 655.00 C"` | |
| `bid` | float | `4.84` | |
| `ask` | float | `4.88` | |
| `last` | float | `4.84` | |
| `mark` | float | `4.86` | |
| `bidSize` | int | `178` | |
| `askSize` | int | `104` | |
| `totalVolume` | int | `63540` | |
| `openInterest` | int | `4113` | |
| `volatility` | float | `15.627` | Implied vol |
| `delta` | float | `0.53` | |
| `gamma` | float | `0.038` | |
| `theta` | float | `-0.563` | |
| `vega` | float | `0.27` | |
| `rho` | float | `0.036` | |
| `strikePrice` | float | `655.0` | |
| `expirationDate` | string | `"2026-04-06T20:00:00..."` | |
| `daysToExpiration` | int | `2` | |
| `expirationType` | string | `"W"` | W=weekly, R=regular, Q=quarterly |
| `multiplier` | float | `100.0` | |
| `intrinsicValue` | float | `0.83` | |
| `extrinsicValue` | float | `4.01` | |
| `inTheMoney` | bool | `true` | |
| `pennyPilot` | bool | `true` | |
| `exerciseType` | string | `"A"` | A=American, E=European |
| `settlementType` | string | `"P"` | P=physical |

---

## What schwagent Uses Today

| Data | Source Endpoint | Used By |
|------|----------------|---------|
| Account type (CASH/MARGIN) | GET /accounts | TradingRules — PDT applicability |
| Round trips | GET /accounts | TradingRules — Schwab's day trade count |
| isDayTrader | GET /accounts | TradingRules — PDT flag |
| isClosingOnlyRestricted | GET /accounts | TradingRules — hard block on buys |
| liquidationValue | GET /accounts | RiskManager — portfolio total, drawdown |
| cashBalance | GET /accounts | RiskManager — available cash for orders |
| unsettledCash | GET /accounts | AccountSummary (tracked, not yet enforced) |
| positions | GET /accounts | All strategies — current holdings |
| OHLCV candles | GET /pricehistory | All strategies — indicator computation |
| Quotes (bid/ask/last) | GET /quotes | ETF rotation — current prices |

## What We Could Use But Don't Yet

| Data | Source | Potential Use |
|------|--------|---------------|
| `fundamental.divExDate` / `nextDivExDate` | GET /quotes | Avoid buying right before ex-div (price drop) |
| `fundamental.lastEarningsDate` | GET /quotes | Earnings avoidance (roadmap item #3) |
| `fundamental.peRatio`, `eps` | GET /quotes | Valuation overlay for stock strategies |
| `fundamental.avg10DaysVolume` | GET /quotes | Liquidity filter — skip illiquid symbols |
| `reference.isShortable` | GET /quotes | Future short strategies |
| Options greeks | GET /chains | Future options strategies / hedging |
| `streamerInfo` | GET /userPreference | Real-time WebSocket streaming (replace polling) |
| `lotSelectionMethod` | GET /userPreference | Tax-aware selling (FIFO vs specific lot) |
| Orders history | GET /accounts/{hash}/orders | Reconciliation, fill price tracking |
| Transactions | GET /accounts/{hash}/transactions | Accurate P&L, cost basis |
