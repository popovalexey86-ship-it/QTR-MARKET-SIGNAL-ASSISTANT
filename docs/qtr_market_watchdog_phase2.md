# QTR Market Watchdog Phase 2

Phase 2 is an explainable anomaly engine. It measures how unusual an observed
market event is. It does not estimate profitability, choose direction, or emit
trade instructions.

## Point-in-time boundary

- A candle is usable only when `candle.timestamp + interval <= detected_at`.
- Every feature satisfies `feature.available_at <= detected_at`.
- Baselines are queried before the current snapshot is committed.
- OI and funding are omitted when their snapshot is missing, future-dated, or
  older than the configured maximum age.
- Missing and stale fields remain explicit; the engine never imputes values.

## Feature formulas

For completed candles only:

- `normalized_range_5 = mean((high-low)/close, last 5)`.
- `range_width_10 = (max(high)-min(low))/last_close, last 10`.
- `range_touch_density_10 = boundary-touching candles / 10`, with a boundary
  band equal to 10% of the local range width.
- `relative_volume_20 = current_volume / mean(previous 20 volumes)`.
- `volume_acceleration_3 = mean(last 3 volumes) / mean(previous 10 volumes)`.
- `price_change_1 = close[t]/close[t-1]-1`.
- `price_acceleration = price_change[t]-price_change[t-1]`.
- `absolute_price_acceleration = abs(price_acceleration)`.
- `realized_volatility_5 = population_std(last 5 one-bar returns)`.
- `open_interest_change` and `funding_rate` come from the existing normalized
  derivatives snapshot through a PIT/freshness adapter.

Each feature is compared only with the rolling baseline of the same symbol and
feature. A baseline is unavailable until its configured minimum sample count is
met.

## Detector engineering defaults

Let `r = current / baseline_mean`. Increasing severity is:

`S+(x,t,c) = 30 + 70 * clip((x-t)/(c-t), 0, 1)`.

Decreasing severity is:

`S-(x,t,f) = 30 + 70 * clip((t-x)/(t-f), 0, 1)`.

These are engineering defaults, not outcome-fitted thresholds.

| Detector | Requirements | Trigger and severity |
|---|---|---|
| Compression | `normalized_range_5`, its ready baseline | `r <= 0.75`; `S-(r,0.75,0.25)` |
| Range buildup | `range_width_10`, ready width baseline, touch density | width `r <= 0.80` and touches `>= 0.40`; mean of width `S-` and touch `S+` |
| Volume shock | `relative_volume_20`, ready baseline | `r >= 1.50`; `S+(r,1.50,4.00)` |
| Volume acceleration | `volume_acceleration_3`, ready baseline | `r >= 1.40`; `S+(r,1.40,3.50)` |
| Price acceleration | signed change/acceleration and ready absolute-acceleration baseline | absolute `r >= 2.00`; `S+(r,2.00,5.00)`; sign is descriptive only |
| Volatility expansion | `realized_volatility_5`, ready baseline | `r >= 1.50`; `S+(r,1.50,4.00)` |
| OI anomaly | fresh OI change, price change, ready OI baseline | `abs(OI change) >= 0.02` and `abs(z) >= 2`; if baseline variance is zero, historical absolute ratio `>= 3` |
| Funding anomaly | fresh funding and ready baseline | `abs(z) >= 2.50`; severity capped at 55 |

OI explanations preserve the four observed regimes `PRICE_UP+OI_UP`,
`PRICE_DOWN+OI_UP`, `PRICE_UP+OI_DOWN`, and `PRICE_DOWN+OI_DOWN`; none is a
LONG/SHORT recommendation.

## Explainable aggregation

For anomaly `i`:

`base_points[i] = configured_weight[i] * severity[i] / 100`.

Weights are compression 18, range buildup 16, volume shock 18, volume
acceleration 24, price acceleration 18, volatility expansion 21, OI 15, and
funding 8.

Within correlated groups, the strongest member receives factor 1.0 and each
secondary member receives an explicit attenuation:

- compression/range buildup: 0.35;
- volume shock/volume acceleration: 0.25;
- price acceleration/volatility expansion: 0.50.

OI and funding are separate context groups. Funding can contribute at most 4.4
points because its detector severity is capped at 55 and its weight is 8.

`anomaly_score = min(100, sum(adjusted_points) + sequence_bonus)`.

The causal sequence bonus is 12 for an earlier compression, then an earlier
volume anomaly, followed by current volatility expansion. An earlier range
buildup followed by current volatility expansion contributes 8. Future steps
are ignored.

## Persistence

The versioned state file atomically persists each symbol's current and previous
state, confirmation counters, cooldown deadline, last transition, last score,
and bounded causal sequence context. Loading the repository reconstructs these
objects exactly, so WATCH and IN_PLAY do not reset to NORMAL after restart.

The liquidation boundary is a protocol only. Phase 2 deliberately does not
create or subscribe to another liquidation source because the existing
accumulator does not yet expose the full PIT provenance required by Watchdog.
