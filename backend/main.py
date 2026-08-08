"""Serves the notebook's 5-panel order-flow chart (price, OI, funding, spot/fut CVD)
straight off the parquet archive. Run: uvicorn main:app --reload"""
import polars as pl
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

ROOT = "../parquet"
E8 = 1e8   # price scale (archive build-plan §1)
E9 = 1e9   # funding rate scale

# Quantity and OI are on the same 1e-8 grid as price only AFTER the collector
# restart on 2026-08-06 22:26 UTC; the rows before it are on 1e-3. Verified
# against bybit's `oi_notional` (implied scale 1.0e3 -> 1.0e8) and by comparing
# um's bar `volume` to its footprint (0.0004 BTC/min -> 23 BTC/min across the
# gap). Without this the OI panel shows a fake 100,000x step.
# ponytail: a constant, because the archive has exactly one such break. If it
# rescales again, derive the scale per row from oi_notional/mark instead.
RESCALED_AT = 1786055160000
QTY = pl.when(pl.col("minute") < RESCALED_AT).then(1e3).otherwise(E8)

# Each venue's own spelling of BTC. Deribit is left out: its perp is inverse
# (size in USD), so it can't be summed into a BTC-denominated CVD.
PERP = {"um": "BTCUSDT", "bybit": "BTCUSDT", "okx": "BTC-USDT-SWAP", "hl": "BTC"}
SPOT = {"spot": "BTCUSDT", "bybitspot": "BTCUSDT", "okxspot": "BTC-USDT", "coinbase": "BTC-USD"}

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def scan(stream):
    return pl.scan_parquet(f"{ROOT}/{stream}")


def bucket(tf_min):
    ms = tf_min * 60_000
    return (pl.col("minute") // ms * ms // 1000).alias("time")


def ohlc_of(col):
    """The four candle aggregates of a per-minute series, within one bucket."""
    return (col.first().alias("open"), col.max().alias("high"),
            col.min().alias("low"), col.last().alias("close"))


def venue_filter(venues):
    if not venues:
        return pl.lit(False)  # every exchange toggled off -- match nothing
    return pl.any_horizontal(
        (pl.col("venue") == v) & (pl.col("symbol") == s) for v, s in venues.items()
    )


def cvd(venues, tf_min):
    """Cumulative taker delta summed across venues, as candles rebased to 0.

    The cumsum runs at minute resolution and is only then bucketed -- bucketing
    first would collapse each candle to a flat open==high==low==close bar.
    """
    per_min = (
        scan("footprint").filter(venue_filter(venues))
        .group_by("minute")
        .agg(((pl.col("buy_qty") - pl.col("sell_qty")) / QTY).sum().alias("d"))
        .sort("minute")
        .with_columns(pl.col("d").cum_sum().alias("v"))
    )
    df = per_min.group_by(bucket(tf_min)).agg(*ohlc_of(pl.col("v"))).sort("time").collect()
    base = df["open"][0]
    return df.with_columns(pl.col(c) - base for c in ("open", "high", "low", "close"))


def oi_agg(venues, tf_min):
    """Open interest summed across venues, as candles."""
    per_min = (
        scan("oi").filter(venue_filter(venues))
        .group_by("minute")
        .agg((pl.col("oi") / QTY).sum().alias("v"))
        .sort("minute")
    )
    return per_min.group_by(bucket(tf_min)).agg(*ohlc_of(pl.col("v"))).sort("time").collect()


def funding_agg(venues, tf_min):
    """Annualised funding rate averaged across venues, one value per bucket."""
    per_min = (
        scan("mark").filter(venue_filter(venues))
        .group_by("minute")
        .agg((pl.col("funding_rate") / E9 * 3 * 365 * 100).mean().alias("v"))
        .sort("minute")
    )
    return (
        per_min.group_by(bucket(tf_min)).agg(pl.col("v").last().alias("value"))
        .drop_nulls().sort("time").collect()
    )


def pick(venues, keys):
    """The subset of a venue->symbol dict named by a comma-separated key list.

    keys=None means the param was omitted -- use every venue. keys="" means
    it was passed empty on purpose -- the user toggled every exchange off.
    """
    if keys is None:
        return dict(venues)
    wanted = keys.split(",") if keys else []
    return {k: venues[k] for k in wanted if k in venues}


@app.get("/api/venues")
def venues():
    """The toggle-able exchanges for each aggregated panel."""
    return {"perp": list(PERP), "spot": list(SPOT)}


@app.get("/api/chart")
def chart(
    venue: str = "um", tf: int = 5,
    oi_venues: str | None = None, funding_venues: str | None = None,
    spot_venues: str | None = None, fut_venues: str | None = None,
):
    sym = PERP[venue]
    t = bucket(tf)

    ohlc = (
        scan("bar").filter((pl.col("venue") == venue) & (pl.col("symbol") == sym))
        .sort("minute")
        .group_by(t)
        .agg(
            (pl.col("px_open").first() / E8).alias("open"),
            (pl.col("px_high").max() / E8).alias("high"),
            (pl.col("px_low").min() / E8).alias("low"),
            (pl.col("px_close").last() / E8).alias("close"),
        )
        .sort("time")
        .collect()
    )

    return {
        "ohlc": ohlc.to_dicts(),
        "oi": oi_agg(pick(PERP, oi_venues), tf).to_dicts(),
        "funding": funding_agg(pick(PERP, funding_venues), tf).to_dicts(),
        "spot_cvd": cvd(pick(SPOT, spot_venues), tf).to_dicts(),
        "fut_cvd": cvd(pick(PERP, fut_venues), tf).to_dicts(),
    }


if __name__ == "__main__":
    d = chart()
    assert all(d[k] for k in d), {k: len(v) for k, v in d.items()}
    b = d["ohlc"][0]
    assert 1_000 < b["low"] <= b["open"] <= b["high"] and b["low"] <= b["close"] <= b["high"], b
    lo = [r["low"] for r in d["oi"]]
    assert all(1e4 < v < 1e7 for v in lo), (min(lo), max(lo))    # BTC, not raw ints, summed across venues
    assert abs(d["funding"][0]["value"]) < 200, d["funding"][0]  # % annualised
    for k in ("oi", "spot_cvd", "fut_cvd"):
        c = d[k][1]
        assert c["low"] <= min(c["open"], c["close"]) and c["high"] >= max(c["open"], c["close"]), (k, c)
        assert c["high"] > c["low"], f"{k} candle is flat -- cumsum bucketed too early"
    assert d["fut_cvd"][0]["open"] == 0, "cvd not rebased"
    # single-venue slice should stay within the OI step's old (pre-aggregation) sanity bound
    solo = oi_agg(pick(PERP, "um"), 5).to_dicts()
    assert all(1e4 < r["low"] < 1e6 for r in solo), "OI step -- the 1e-3/1e-8 rescale is not handled"
    print({k: len(v) for k, v in d.items()}, "ok")
