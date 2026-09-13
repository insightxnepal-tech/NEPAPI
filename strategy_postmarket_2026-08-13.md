# NEPSE Post-Market Strategy — 2026-08-13

**Session turnover:** Rs 3.72B  |  **Median stock move:** -0.51%
**Active scrips:** 347
**Selected model:** `sniper` (highest historical accuracy)

## Model accuracy (next-day, last ~15 sessions)

| Model | Hit rate | Avg next-day return | Samples |
|-------|----------|---------------------|---------|
| continuation | 8.0% | -4.18% | 75 |
| smart_money | 30.7% | -1.62% | 75 |
| sniper ← selected | 45.3% | -1.07% | 75 |
| composite | 13.3% | -3.68% | 75 |

## Highest-conviction next-session plays (top 5 by sniper)

| Symbol | Close | Intraday | VWAP Δ | Vol x | Score | Entry | SL | T1 | T2 | Signal |
|--------|-------|----------|--------|-------|-------|-------|----|----|----|--------|
| **ALBSL** | Rs 1071.0 | -4.80% | -4.06% | 1.53x | 80 | Rs 1071.0 | Rs 956.61 | Rs 1223.51 | Rs 1337.9 | 👀 NEAR BUY |
| **NRN** | Rs 1501.5 | +5.75% | +5.16% | 0.92x | 60 | Rs 1501.5 | Rs 1356.74 | Rs 1694.51 | Rs 1839.28 | NEUTRAL |
| **RADHI** | Rs 766.2 | +6.42% | +6.38% | 0.41x | 60 | Rs 766.2 | Rs 698.65 | Rs 856.27 | Rs 923.82 | NEUTRAL |
| **RIDI** | Rs 393.7 | +6.12% | +5.82% | 0.30x | 60 | Rs 393.7 | Rs 355.99 | Rs 443.99 | Rs 481.7 | NEUTRAL |
| **API** | Rs 333.9 | +0.69% | +0.82% | 0.72x | 60 | Rs 333.9 | Rs 315.55 | Rs 358.37 | Rs 376.72 | NEUTRAL |

## Today's realized leaders

| Symbol | Close | Intraday | Turnover | Range pos | Close vs VWAP |
|--------|-------|----------|----------|-----------|---------------|
| RADHI | Rs 766.2 | +6.42% | Rs 26.9M | 1.00 | +6.38% |
| RIDI | Rs 393.7 | +6.12% | Rs 97.0M | 1.00 | +5.82% |
| NRN | Rs 1501.5 | +5.75% | Rs 166.9M | 1.00 | +5.16% |

## Today's laggards (liquidity-filtered)

| Symbol | Close | Intraday | Turnover |
|--------|-------|----------|----------|
| SKHL | Rs 840.0 | -15.83% | Rs 45.2M |
| GHL | Rs 233.9 | -8.88% | Rs 299.8M |
| LEC | Rs 207.0 | -7.80% | Rs 60.1M |

## Playbook

- Use **sniper** rankings — it won the walk-forward accuracy test (45.3% hit-rate, -1.07% avg next-day).
- Prefer names that closed in the upper half of the day's range **and** above VWAP.
- Skip illiquid prints (< Rs 15M turnover or < 30 trades).
- Stops: sniper ATR stop when available, otherwise ~3% below close.
- **Cash bias:** the winning model is still negative-expectancy in this tape. Do not chase today's leaders (continuation hit-rate is the worst). Half-size only, or skip new entries.

*Auto-generated from NEPSE floorsheet data. Not financial advice.*
