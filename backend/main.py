"""Serves the notebook's 5-panel order-flow chart (price, OI, funding, spot/fut CVD)
straight off the parquet archive. Run: uvicorn main:app --reload"""
import re

import polars as pl
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

ROOT = "../parquet"
# Price, quantity and OI all share one 1e-8 integer grid (archive build-plan §1).
# Verified on every venue: bybit's oi_notional/oi == mark_px/1e8 (64940.46), and
# um's footprint sums to its bar `volume` (21.24 vs 21.27 BTC/min). The archive
# starts 2026-08-06 23:59, after the collector's 22:26 rescale, so no row is on
# the older 1e-3 grid -- the self-check's OI magnitude bound catches it if an
# earlier window is ever backfilled.
E8 = 1e8   # price / quantity / OI scale
E9 = 1e9   # funding rate scale

# How each venue spells a coin. Group 1 is a quantity multiplier: binance and
# bybit quote some memecoins in units of 1000 (1000PEPEUSDT), so a raw sum would
# be in thousands of coins, not coins. Group 2 is the coin itself.
#
# Symbols containing an underscore are skipped unless the venue is in
# UNDERSCORE_OK. The skip drops um's dated futures (BTCUSDT_260925) -- velo's
# coin aggregates are perpetuals only -- and deribit's USDC-margined linear
# perps (BTC_USDC-PERPETUAL), which velo excludes too: its deribit universe is
# BTC-PERPETUAL and ETH-PERPETUAL, the inverse pair. Without the skip,
# BTC_USDC-PERPETUAL (431 BTC) parses to coin "BTC" and displaces the real
# BTC-PERPETUAL (11,041). deribitspot is exempt because an underscore is simply
# how it spells the quote currency, and its grammar terminates in _USDC.
SYMBOL_RE = {
    "um": r"^(\d*)(.+)USDT$", "bybit": r"^(\d*)(.+)USDT$",
    "okx": r"^(\d*)(.+)-USDT-SWAP$", "hl": r"^(\d*)(.+)$",
    "deribit": r"^(\d*)(.+)-PERPETUAL$",
    "spot": r"^(\d*)(.+)USDT$", "bybitspot": r"^(\d*)(.+)USDT$",
    "okxspot": r"^(\d*)(.+)-USDT$", "coinbase": r"^(\d*)(.+)-USD$",
    "deribitspot": r"^(\d*)(.+)_USDC$",
}
UNDERSCORE_OK = {"deribitspot"}

# Futures venues carry OI and futures CVD; spot venues carry spot CVD.
# deribitspot is thin -- 0.15% of BTC spot flow, under 0.05% on ETH/SOL/XRP --
# and velo leaves it out of its spot universe, so ticking it off restores an
# exact velo match on the spot CVD pane.
FUT_VENUES = ("um", "bybit", "okx", "hl", "deribit")
# Liquidation feeds. hl and deribit publish no public forced-order stream, so
# this pane is three venues wide where OI is five -- not a wiring gap.
LIQ_VENUES = ("um", "bybit", "okx")
# Options venues. `opt` is binance-options (BTC-260807-61000-P, YYMMDD strikes),
# `deribitopt` is deribit (BTC-10AUG26-58000-P). Keyed off `underlying`, so no
# symbol grammar: only BTC and ETH exist, and deribit lists BTC alone.
OPT_VENUES = ("opt", "deribitopt")
SPOT_VENUES = ("spot", "bybitspot", "okxspot", "coinbase", "deribitspot")

# For BTC, velo files EIGHT products under coin=BTC and the archive carries five.
# The three coin-margined perps below are not collected at all (~31k BTC, ~13%):
#   binance-coin-margin BTCUSD_PERP   ~16.5k BTC  dapi.binance.com /dapi/v1/openInterest
#   bybit-coin-margin   BTCUSD         ~7.4k BTC  api.bybit.com  category=inverse
#   okex-coin-margin    BTC-USD-SWAP   ~7.2k BTC  okx.com /api/v5/public/open-interest
# Nothing in this file can close that; the collector has to subscribe to them.

EMPTY_CANDLES = pl.DataFrame(schema={"time": pl.Int64} | {c: pl.Float64 for c in ("open", "high", "low", "close")})
EMPTY_LINE = pl.DataFrame(schema={"time": pl.Int64, "value": pl.Float64})
EMPTY_LIQ = pl.DataFrame(schema={"time": pl.Int64, "long": pl.Float64, "short": pl.Float64})
EMPTY_OPTOI = pl.DataFrame(schema={"time": pl.Int64, "call": pl.Float64, "put": pl.Float64})


def scan(stream):
    return pl.scan_parquet(f"{ROOT}/{stream}")


def parse_symbol(venue, symbol):
    """(multiplier, coin) for a venue's symbol, or None if it is not a perp/spot pair."""
    if "_" in symbol and venue not in UNDERSCORE_OK:
        return None
    m = re.match(SYMBOL_RE[venue], symbol)
    return (int(m.group(1) or 1), m.group(2)) if m else None


def build_catalogue():
    """{coin: {"mult": int, "fut": {venue: symbol}, "spot": {venue: symbol}}}

    Discovered from the archive rather than hand-listed: 107 coins across 5
    futures and 4 spot venues is not a table worth maintaining by hand, and a
    hand-written one goes stale the moment the collector adds a market.

    `mult` is one number per coin, not per venue, because every venue that lists
    a 1000x memecoin agrees on the multiplier -- asserted in the self-check, so a
    future archive that breaks that assumption fails loudly instead of summing
    1000x-scaled OI onto unscaled OI.
    """
    cat = {}
    for stream, venues, group in (("oi", FUT_VENUES, "fut"), ("footprint", SPOT_VENUES, "spot"),
                                  ("liq", LIQ_VENUES, "liq")):
        pairs = scan(stream).select("venue", "symbol").unique().collect()
        for venue, symbol in pairs.iter_rows():
            if venue not in venues:
                continue
            p = parse_symbol(venue, symbol)
            if not p:
                continue
            mult, coin = p
            c = cat.setdefault(coin, {"mult": mult, "fut": {}, "spot": {}, "liq": {}, "opt": []})
            c["mult"] = max(c["mult"], mult)
            # One venue must not offer two symbols for the same coin: the second
            # would silently displace the first and the panel would quietly show
            # the wrong book. Fail instead, so the grammar above gets tightened.
            if c[group].get(venue, symbol) != symbol:
                raise ValueError(
                    f"{venue} maps both {c[group][venue]!r} and {symbol!r} to coin {coin!r}; "
                    f"SYMBOL_RE[{venue!r}] is ambiguous")
            c[group][venue] = symbol
    # Options are keyed by `underlying` rather than a per-venue symbol: one coin
    # spans many expiries, so the venue list is what a toggle selects.
    for coin, venue in (scan("optoi").select("underlying", "venue").unique()
                        .collect().iter_rows()):
        if venue in OPT_VENUES and coin in cat:
            cat[coin]["opt"].append(venue)
    for c in cat.values():
        c["opt"] = sorted(set(c["opt"]))
    return dict(sorted(cat.items()))


def build_funding_hours():
    """{(venue, symbol): hours} -- the funding period, read off the venue's own
    schedule rather than hard-coded.

    Funding intervals are not a per-venue constant: hyperliquid settles hourly
    while binance/bybit/okx settle 8-hourly, and binance runs 4h on some symbols.
    orderflow-alpha's binance.get_funding_rate infers it the same way, from the
    spacing between settlements. A market whose schedule shows fewer than two
    distinct settlements is left out entirely -- deribit is the case here, and
    guessing its period would corrupt the panel rather than extend it.
    """
    sched = (
        scan("mark").group_by("venue", "symbol")
        .agg(pl.col("next_funding_ts").unique().sort().diff().median().alias("ms"),
             pl.col("next_funding_ts").n_unique().alias("n"))
        .collect()
    )
    return {
        (r["venue"], r["symbol"]): r["ms"] / 3.6e6
        for r in sched.to_dicts()
        if r["n"] > 1 and r["ms"] and r["ms"] % 3.6e6 == 0
    }


CATALOGUE = build_catalogue()
FUNDING_HRS = build_funding_hours()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def bucket(tf_min):
    ms = tf_min * 60_000
    return (pl.col("minute") // ms * ms // 1000).alias("time")


def ohlc_of(col, prev):
    """The four candle aggregates of a per-minute series, within one bucket.

    `open` is the previous bucket's close -- `prev` is the series shifted one
    minute, so its first value inside a bucket is the last value before it --
    and never the first sample *in* the bucket.

    These panels are running series: CVD is a cumulative sum and OI a polled
    level, so both are continuous by construction and a candle has to open
    exactly where the last one closed. Taking the first in-bucket sample as the
    open drops the move from the previous close into the crack between two
    candles, where no candle draws it: 553 of 576 spot-CVD candles used to open
    away from the prior close, which reads as the series gapping every bar.
    Price is not built through here -- real trades do gap across a bar edge.
    """
    o = pl.coalesce(prev.first(), col.first())  # first bucket has no close before it
    return (o.alias("open"),
            pl.max_horizontal(o, col.max()).alias("high"),
            pl.min_horizontal(o, col.min()).alias("low"),
            col.last().alias("close"))


def venue_filter(venues):
    if not venues:
        return pl.lit(False)  # every exchange toggled off -- match nothing
    return pl.any_horizontal(
        (pl.col("venue") == v) & (pl.col("symbol") == s) for v, s in venues.items()
    )


def minute_grid(df):
    """An unbroken 1-minute index spanning `df`, to reindex a series onto.

    The collector went down twice in this archive (20 min and 99 min on
    2026-08-07), so the minutes that exist in a stream are not the minutes the
    panel needs. Both orderflow-alpha paths rebuild a complete index before
    filling -- features._open_interest via date_range, the CVD ones implicitly
    via pandas .resample(), which emits every bucket in range.
    """
    return pl.select(
        pl.int_range(df["minute"].min(), df["minute"].max() + 1, 60_000).alias("minute")
    )


def cvd(venues, tf_min, mult=1):
    """Cumulative taker delta summed across venues, as candles rebased to 0.

    The cumsum runs at minute resolution and is only then bucketed -- bucketing
    first would collapse each candle to a flat open==high==low==close bar.

    A minute with no rows means no trade was *observed*, so its delta is 0 and
    the line carries flat -- same as the reference's .resample().sum().cumsum().
    Across the two outages that is a plateau, not a gap: flow certainly happened,
    we just did not record it, and inventing a slope would be worse than a flat.
    """
    per_min = (
        scan("footprint").filter(venue_filter(venues))
        .group_by("minute")
        .agg(((pl.col("buy_qty") - pl.col("sell_qty")) / E8 * mult).sum().alias("d"))
        .collect()
    )
    if per_min.is_empty():  # every exchange toggled off -- the panel goes blank, not 500
        return EMPTY_CANDLES
    per_min = (
        minute_grid(per_min).join(per_min, on="minute", how="left")
        .sort("minute")  # cum_sum/shift below are order-dependent; a join is not ordered
        .with_columns(pl.col("d").fill_null(0.0))
        .with_columns(pl.col("d").cum_sum().alias("v"))
    )
    df = candles(per_min.select("minute", "v"), tf_min)
    base = df["open"][0]
    return df.with_columns(pl.col(c) - base for c in ("open", "high", "low", "close"))


def aligned(stream, venues, value, fill):
    """One row per minute, combining a *level* across series on a common grid.

    OI and funding are levels, not flows, so the venues have to be lined up
    before they are combined. Each venue polls on its own ~60s timer with enough
    jitter to drift across a minute boundary (um's snapshot spacing runs 55.999s
    to 60.022s): one minute gets two snapshots from a venue and its neighbour
    gets none. Combining the rows where they fall then counts that venue twice
    in one minute and not at all in the next -- on OI that is a sawtooth the
    size of the venue's whole book (+-107k BTC against a ~200k total).

    Returns one column per venue, so the caller decides how to combine them --
    OI sums, funding takes an OI-weighted average.

    The grid is rebuilt as an unbroken minute range rather than taken from the
    rows present, because the collector itself went down twice in this archive
    (20 min at 14:06 and 99 min at 20:19 on 2026-08-07, every venue at once).
    Without that the panel comes out with holes in it. This is orderflow-alpha's
    features._open_interest, which reindexes onto a full date_range before
    filling, rather than resampling only the timestamps that exist.

    `fill` follows the same reference: OI interpolates (a continuous level moves
    smoothly between two known readings) while funding forward-fills (a rate
    holds until the next settlement, so the last reading is the right answer).
    ponytail: a 99-minute interpolation is 18 invented 5m bars -- fine for a
    shape-reading panel, wrong if anything ever trades off these bars.
    """
    per_key = (
        scan(stream).filter(venue_filter(venues))
        .sort("ts_recv")
        .group_by("minute", pl.col("venue").alias("k"))
        .agg(value.last().alias("v"))
        .collect()
    )
    if per_key.is_empty():  # every exchange toggled off
        return per_key.select("minute")
    wide = per_key.pivot("k", index="minute", values="v").sort("minute")
    cols = pl.exclude("minute")
    return (
        minute_grid(wide).join(wide, on="minute", how="left")
        .sort("minute")  # interpolate/forward-fill are order-dependent; a join is not ordered
        .with_columns(cols.interpolate() if fill == "interpolate" else cols.fill_null(strategy="forward"))
        .drop_nulls()  # leading minutes, before a venue's first snapshot
    )


def oi_open_interest(venues, mult=1):
    """Per-venue open interest in coins, on the aligned minute grid.

    Divide before scaling: `oi` is Int64 and a 1000x memecoin's raw open
    interest is ~6.7e17, so `oi * 1000` overflows Int64 (max 9.2e18) and wraps
    silently to a plausible-looking wrong number. `/ E8` makes it Float64 first.
    """
    return aligned("oi", venues, pl.col("oi") / E8 * mult, "interpolate")


def candles(per_min, tf_min):
    """Bucket a per-minute running series into continuous candles."""
    per_min = per_min.sort("minute").with_columns(pl.col("v").shift(1).alias("p"))
    return per_min.group_by(bucket(tf_min)).agg(*ohlc_of(pl.col("v"), pl.col("p"))).sort("time")


def oi_agg(venues, tf_min, mult=1):
    """Open interest summed across venues, as candles."""
    w = oi_open_interest(venues, mult)
    if w.width < 2:  # every exchange toggled off
        return EMPTY_CANDLES
    return candles(w.select("minute", pl.sum_horizontal(pl.exclude("minute")).alias("v")), tf_min)


def liq_agg(venues, tf_min, mult=1):
    """Liquidated size per bucket, split long vs short, in coins.

    A flow, not a level: a bucket with no liquidations really is zero, so this
    fills gaps with 0 rather than carrying the last value the way OI does.

    side 0 is a liquidated SHORT and side 1 a liquidated LONG. bybit and okx say
    so outright in `pos_side`; binance leaves that column null, so the encoding
    was checked against price instead -- at the minute of each event the mean
    5-minute return is +9.8bp on side 0 and -8.7bp on side 1, the same split and
    the same sign as the two venues that label it (+15.6/-10.4, +14.0/-11.2).
    Longs are the ones that die into a falling tape.
    """
    per_min = (
        scan("liq").filter(venue_filter(venues))
        .group_by("minute")
        .agg(
            (pl.when(pl.col("side") == 1).then(pl.col("qty")).otherwise(0) / E8 * mult).sum().alias("long"),
            (pl.when(pl.col("side") == 0).then(pl.col("qty")).otherwise(0) / E8 * mult).sum().alias("short"),
        )
        .collect()
    )
    if per_min.is_empty():
        return EMPTY_LIQ
    return (
        minute_grid(per_min).join(per_min, on="minute", how="left")
        .with_columns(pl.col("long").fill_null(0.0), pl.col("short").fill_null(0.0))
        .group_by(bucket(tf_min))
        .agg(pl.col("long").sum(), pl.col("short").sum())
        .sort("time")
    )


def optoi_agg(coin, venues, tf_min):
    """Options open interest in coins, calls and puts, across expiries and venues.

    Contract size is one coin -- oi_notional/(oi_call+oi_put) comes out at the
    spot price (64,927.75) -- so `/E8` is already coin-denominated.

    Aligned per venue+expiry rather than per venue, because binance posts a
    median of 4 of its 13 expiries in a given minute (deribit posts all 12);
    summing the rows that happen to land would swing the total by most of the
    surface. But unlike the perp panes an expiry cannot simply be carried
    forward forever: the listed set turns over inside the archive -- 20260807
    settles at 08:00 on the 7th, 20260811 is listed at 08:05 -- so each expiry
    is held only between its first and last sighting and contributes nothing
    outside that. Requiring all of them at once, as the perp panes do, would
    clip the panel to the window where every expiry overlapped: 16 bars of 48.
    """
    if not venues:
        return EMPTY_OPTOI
    raw = (
        scan("optoi")
        .filter((pl.col("underlying") == coin) & pl.col("venue").is_in(list(venues)))
        .sort("ts_recv")
        .group_by("minute", (pl.col("venue") + "|" + pl.col("symbol")).alias("k"))
        .agg((pl.col("oi_call") / E8).last().alias("call"), (pl.col("oi_put") / E8).last().alias("put"))
        .collect()
    )
    if raw.is_empty():
        return EMPTY_OPTOI
    span = raw.group_by("k").agg(pl.col("minute").min().alias("lo"), pl.col("minute").max().alias("hi"))
    per_min = (
        minute_grid(raw).join(span, how="cross")
        .filter(pl.col("minute").is_between(pl.col("lo"), pl.col("hi")))
        .join(raw, on=["minute", "k"], how="left")
        .sort("k", "minute")
        .with_columns(pl.col("call").forward_fill().over("k"), pl.col("put").forward_fill().over("k"))
        .group_by("minute").agg(pl.col("call").sum(), pl.col("put").sum())
    )
    return (
        per_min.group_by(bucket(tf_min))
        .agg(pl.col("call").last(), pl.col("put").last())
        .sort("time")
    )


def funding_agg(venues, tf_min):
    """Annualised funding rate across venues, open-interest weighted.

    Two things have to be right for this to line up with an aggregator:

    1. `funding_rate` is the rate for ONE funding period, and the period is not
       the same everywhere -- hyperliquid settles hourly, the rest 8-hourly -- so
       each venue is annualised by its own payment count before being combined.
       Annualising the whole panel at 3/day reads hyperliquid 8x low (3.4%/yr as
       0.43%). orderflow-alpha's binance.get_funding_rate infers the same way:
       `periods_per_year = (24 / interval_hours) * 365`.

    2. The venues are combined by an OI-weighted average, not a plain mean.
       Velo's docs: "the rate shown for each period is an open-interest weighted
       average of a coin's funding rate ... across all supported exchanges". An
       equal-weight mean gives hyperliquid's book the same say as binance's,
       which is ~3.5x larger -- worth ~0.85pp/yr on this archive.
    """
    hrs = {v: FUNDING_HRS[(v, s)] for v, s in venues.items() if (v, s) in FUNDING_HRS}
    if not hrs:
        return EMPTY_LINE
    venues = {v: s for v, s in venues.items() if v in hrs}
    f = aligned("mark", venues, pl.col("funding_rate") / E9, "forward")
    w = oi_open_interest(venues)
    keys = [v for v in venues if v in f.columns and v in w.columns]
    if not keys:
        return EMPTY_LINE
    # Annualise per venue *after* the pivot: each venue is its own column by
    # then, so its period is a plain scalar. Doing it before, as a row-wise
    # replace_strict on the venue column, is the same number 300x slower --
    # 4.84s of a 5.00s request, because it re-resolves the map per group.
    j = f.join(w, on="minute", how="inner", suffix="~oi")
    ann = {v: 24 / hrs[v] * 365 * 100 for v in keys}
    weighted = (
        pl.sum_horizontal([pl.col(v) * ann[v] * pl.col(f"{v}~oi") for v in keys])
        / pl.sum_horizontal([pl.col(f"{v}~oi") for v in keys])
    )
    return (
        j.select("minute", weighted.alias("v"))
        .group_by(bucket(tf_min)).agg(pl.col("v").last().alias("value"))
        .drop_nulls().sort("time")
    )


def pick_list(venues, keys):
    """`pick` for a plain venue list -- options select venues, not symbols."""
    if keys is None:
        return list(venues)
    return [v for v in (keys.split(",") if keys else []) if v in venues]


def pick(venues, keys):
    """The subset of a venue->symbol dict named by a comma-separated key list.

    keys=None means the param was omitted -- use every venue. keys="" means
    it was passed empty on purpose -- the user toggled every exchange off.
    """
    if keys is None:
        return dict(venues)
    wanted = keys.split(",") if keys else []
    return {k: venues[k] for k in wanted if k in venues}


# Every whole-minute divisor of a day, 1m to 1d. Buckets are floored off the
# epoch, so a timeframe that does not divide 1440 would straddle midnight and
# silently mis-bin the first bar of every day.
TIMEFRAMES = [tf for tf in (1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, 1440)]


def markets_of(coin):
    """Per-pane venue maps for one coin: futures, funding-capable, spot, liq."""
    e = CATALOGUE[coin]
    fut = e["fut"]
    return fut, {v: s for v, s in fut.items() if (v, s) in FUNDING_HRS}, e["spot"], e["liq"]


@app.get("/api/markets")
def markets():
    """Every coin in the archive, with the exchanges available per panel.

    `mult` is how many coins one contract represents (1000 for the memecoins
    binance/bybit quote as 1000PEPEUSDT); the panels are already scaled by it,
    it is reported so the units on screen are not a mystery.
    """
    out = {}
    for coin, e in CATALOGUE.items():
        fut, funding, spot, liq = markets_of(coin)
        if not (fut or spot):
            continue
        out[coin] = {"mult": e["mult"], "fut": list(fut), "perp": list(funding),
                     "spot": list(spot), "liq": list(liq), "opt": list(e["opt"]),
                     "price": list(fut)}
    return {"coins": out, "timeframes": TIMEFRAMES}


@app.get("/api/chart")
def chart(
    coin: str = "BTC", venue: str | None = None, tf: int = 5,
    oi_venues: str | None = None, funding_venues: str | None = None,
    spot_venues: str | None = None, fut_venues: str | None = None,
    liq_venues: str | None = None, opt_venues: str | None = None,
):
    if coin not in CATALOGUE:
        raise HTTPException(422, f"unknown coin {coin!r}; see /api/markets")
    if tf not in TIMEFRAMES:
        raise HTTPException(422, f"tf must be one of {TIMEFRAMES}, got {tf}")

    fut, perp, spot, liq = markets_of(coin)
    mult = CATALOGUE[coin]["mult"]
    if venue is None:
        venue = "um" if "um" in fut else next(iter(fut), None)
    if venue not in fut:
        raise HTTPException(422, f"{coin} is not on venue {venue!r}; expected one of {list(fut)}")

    ohlc = (
        scan("bar").filter((pl.col("venue") == venue) & (pl.col("symbol") == fut[venue]))
        .sort("minute")
        .group_by(bucket(tf))
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
        "oi": oi_agg(pick(fut, oi_venues), tf, mult).to_dicts(),
        "funding": funding_agg(pick(perp, funding_venues), tf).to_dicts(),
        "spot_cvd": cvd(pick(spot, spot_venues), tf, mult).to_dicts(),
        "fut_cvd": cvd(pick(fut, fut_venues), tf, mult).to_dicts(),
        "liq": liq_agg(pick(liq, liq_venues), tf, mult).to_dicts(),
        "optoi": optoi_agg(coin, pick_list(CATALOGUE[coin]["opt"], opt_venues), tf).to_dicts(),
    }


if __name__ == "__main__":
    FUT, PERP, SPOT, LIQ = markets_of("BTC")

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

    # A cumulative sum and a polled level are both continuous, so every candle
    # must open exactly on the previous close -- no gaps between bars, and the
    # move into a bar has to land inside that bar's own high/low.
    for k in ("oi", "spot_cvd", "fut_cvd"):
        for a, b in zip(d[k], d[k][1:]):
            assert abs(b["open"] - a["close"]) < 1e-9, f"{k} gaps at {b['time']}: {a['close']} -> {b['open']}"
            assert b["low"] <= b["open"] <= b["high"], f"{k} open outside its own range at {b['time']}"

    # No minute may gain or lose a venue's whole book (~30k BTC on the smallest)
    # from poller drift alone. Real OI does not move 15% in 60 seconds.
    v = [r["close"] for r in oi_agg(FUT, 1).to_dicts()]
    jump = max(abs(b - a) for a, b in zip(v, v[1:]))
    assert jump < 0.15 * min(v), f"OI spike of {jump:,.0f} BTC -- venues not aligned before summing"

    # hl funds hourly, the rest 8-hourly. Annualising it at 3/day reads 0.43%/yr
    # instead of 3.4%, which is well outside the band the 8h venues sit in.
    assert FUNDING_HRS[("hl", "BTC")] == 1 and FUNDING_HRS[("um", "BTCUSDT")] == 8, "BTC funding periods moved"
    hl = [r["value"] for r in funding_agg(pick(PERP, "hl"), 60).to_dicts()]
    assert 1 < sum(hl) / len(hl) < 20, f"hl funding {sum(hl)/len(hl):.2f}%/yr -- annualised on the wrong period"

    # Velo weights the per-venue rates by open interest, not equally. um carries
    # ~53% of the OI, so the aggregate must land nearer um's own rate than a flat
    # mean does. This is what fails if the weighting is ever dropped.
    def mean_of(vs):
        r = [x["value"] for x in funding_agg(vs, 5).to_dicts()]
        return sum(r) / len(r)
    solo = {v: mean_of(pick(PERP, v)) for v in PERP}
    agg, flat = mean_of(PERP), sum(solo.values()) / len(solo)
    assert min(solo.values()) <= agg <= max(solo.values()), f"funding {agg} outside per-venue range {solo}"
    assert abs(agg - solo["um"]) < abs(flat - solo["um"]), (
        f"funding is not OI-weighted: agg={agg:.3f} flat={flat:.3f} um={solo['um']:.3f}")

    # Liquidations are a flow, so nothing may be created or lost by bucketing:
    # the pane must sum to exactly the raw archive, split included. This is what
    # catches a mis-mapped `side`, a dropped venue, or double counting.
    raw = scan("liq").filter(venue_filter(LIQ)).select("side", "qty").collect()
    want = {sd: (raw.filter(pl.col("side") == sd)["qty"] / E8).sum() for sd in (0, 1)}
    got = liq_agg(LIQ, 1440).to_dicts()
    assert abs(sum(r["long"] for r in got) - want[1]) < 1e-6, "liq long total does not reconcile"
    assert abs(sum(r["short"] for r in got) - want[0]) < 1e-6, "liq short total does not reconcile"
    assert all(r["long"] >= 0 and r["short"] >= 0 for r in got), "liq magnitudes must be unsigned"

    # side 0 = short, 1 = long. bybit/okx label it in pos_side; assert that, so a
    # collector that flips the encoding fails here rather than inverting the pane.
    lab = (
        scan("liq").filter(pl.col("pos_side").is_not_null())
        .group_by("side").agg(pl.col("pos_side").unique().alias("p")).collect()
    )
    for r in lab.to_dicts():
        assert r["p"] == (["short"] if r["side"] == 0 else ["long"]), f"liq side encoding moved: {r}"

    # Options OI must span the whole archive, not just the window where every
    # expiry happened to overlap -- the listed set turns over inside it.
    oo = optoi_agg("BTC", CATALOGUE["BTC"]["opt"], 60).to_dicts()
    assert len(oo) >= len(d["oi"]) // 60 * 0.9, f"optoi clipped to {len(oo)} bars"
    assert all(1e4 < r["call"] < 1e7 and 1e4 < r["put"] < 1e7 for r in oo), "optoi not in coins"
    assert all(r["call"] > r["put"] for r in oo), "BTC call OI should exceed put OI in this archive"

    # The collector's two outages must not survive as holes in the panel: every
    # bucket between the first and last must be present, at every timeframe.
    for tf in (1, 5, 15, 60, 1440):
        for k, rows in (("oi", oi_agg(FUT, tf)), ("funding", funding_agg(PERP, tf)),
                        ("spot_cvd", cvd(SPOT, tf)), ("fut_cvd", cvd(FUT, tf)),
                        ("liq", liq_agg(LIQ, tf)), ("optoi", optoi_agg("BTC", CATALOGUE["BTC"]["opt"], tf))):
            t = [r["time"] for r in rows.to_dicts()]
            holes = [(a, b) for a, b in zip(t, t[1:]) if b - a > tf * 60]
            assert not holes, f"{k} has {len(holes)} gap(s) at tf={tf}m: {holes[:2]}"

    # Aligning the venues must fill the drifted minutes, not discard them: a
    # "fix" that dropped incomplete minutes would pass every check above.
    src = scan("oi").filter(venue_filter(FUT)).select(pl.col("minute").n_unique()).collect().item()
    kept = oi_open_interest(FUT).height
    assert kept >= src - 2, f"alignment dropped {src - kept} of {src} minutes -- filling, not discarding"

    # Every venue quoting a 1000x memecoin must agree on the multiplier, or the
    # sum adds scaled OI to unscaled OI and the panel is silently 1000x wrong.
    for coin, e in CATALOGUE.items():
        mults = {parse_symbol(v, s)[0] for v, s in (e["fut"] | e["spot"]).items()}
        assert len(mults) == 1, f"{coin}: venues disagree on contract multiplier {mults}"

    # A 1000x memecoin's raw oi is ~6.7e17, so scaling it in the Int64 domain
    # (oi * 1000) overflows 9.2e18 and wraps to a plausible-looking wrong number
    # instead of raising. Reconcile a single-venue one against a float-domain
    # sum computed independently; any overflow shows up as a mismatch.
    coin = next(c for c, e in CATALOGUE.items() if e["mult"] > 1 and len(e["fut"]) == 1)
    e = CATALOGUE[coin]
    raw = (
        scan("oi").filter(venue_filter(e["fut"])).sort("ts_recv")
        .group_by("minute").agg((pl.col("oi").cast(pl.Float64) / E8 * e["mult"]).last().alias("v"))
        .collect()
    )
    want = {r["minute"] // 60_000 * 60: r["v"] for r in raw.to_dicts()}
    got = {r["time"]: r["close"] for r in oi_agg(e["fut"], 1, e["mult"]).to_dicts()}
    shared = set(want) & set(got)
    assert len(shared) > 100, f"{coin}: only {len(shared)} comparable minutes"
    worst = max(abs(got[t] - want[t]) / want[t] for t in shared)
    assert worst < 1e-9, f"{coin} OI off by {worst:.2%} -- Int64 overflow in the multiplier scaling"

    # A spread of coins must serve at the extreme timeframes with the panels
    # still continuous: the biggest, a 1000x memecoin, and the thinnest listings.
    thin = sorted(CATALOGUE, key=lambda c: len(CATALOGUE[c]["fut"]) + len(CATALOGUE[c]["spot"]))[:3]
    for coin in ["BTC", "ETH", "PEPE", "SHIB", *thin]:
        for tf in (1, 1440):
            c = chart(coin=coin, tf=tf)
            for k in ("oi", "spot_cvd", "fut_cvd"):
                for x, y in zip(c[k], c[k][1:]):
                    assert abs(y["open"] - x["close"]) < 1e-6, f"{coin} {k} tf={tf} gaps at {y['time']}"
    print(f"{ {k: len(v) for k, v in d.items()} } {len(CATALOGUE)} coins x {len(TIMEFRAMES)} timeframes ok")
