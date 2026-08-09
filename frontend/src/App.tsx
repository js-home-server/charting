import { useEffect, useMemo, useRef, useState } from 'react'
import {
  createChart, CandlestickSeries, HistogramSeries, LineSeries,
  type IChartApi, type ISeriesApi, type SeriesType, type UTCTimestamp,
} from 'lightweight-charts'
import './App.css'

// TradingView theme, same candle constants as the notebook's plotly version.
// GRID is darkened off the notebook's #E0E3EB: it was lighter than this
// background and vanished.
const BG = '#DBDBDB', GRID = '#BFBFBF', TEXT = '#131722'
const UP = '#2196F3', DOWN = '#F23645'
const API = 'http://localhost:8000'

type Bar = { time: UTCTimestamp; open: number; high: number; low: number; close: number }
type Row = { time: UTCTimestamp } & Record<string, number>
type Data = Record<string, Bar[] | Row[]>
// Which exchanges each panel can aggregate, per coin. Not every coin trades on
// every venue: `perp` is narrower than `fut` (only venues whose funding period
// the archive can confirm), `liq` is narrower still (hl and deribit publish no
// forced-order feed), and `opt` exists only for BTC and ETH.
type Coin = {
  mult: number
  fut: string[]; perp: string[]; spot: string[]; liq: string[]; opt: string[]; price: string[]
}
type Markets = { coins: Record<string, Coin>; timeframes: number[] }

// Relative pane heights. setStretchFactor, not setHeight: setHeight rescales
// every other pane proportionally to make room, so five sequential calls never
// land on the five values you asked for -- it left the four aggregate panes at
// ~55px each against a 740px price pane. Stretch factors are declarative and
// converge, and they survive a window resize.
const PRICE_STRETCH = 360
const PANE_STRETCH = 150

// pane 0 is price; the rest stack under it in this order. `group` says which
// exchange list a panel aggregates and `param` is the query key that selects
// them. `series` is what gets drawn: levels candle like price, flows read as
// bars from zero, and a pane can hold more than one -- liquidations and options
// OI are both two-sided, and netting them would hide the case that matters
// (a cascade liquidating both sides, a put bid against a call wall).
//   field  -- which key of the row to plot; absent means the row IS a candle
//   negate -- draw below the zero line, so longs and shorts oppose each other
const PANES = [
  { key: 'oi', short: 'OI', title: 'Open Interest', group: 'fut', param: 'oi_venues',
    series: [{ type: 'candle' as const }] },
  { key: 'funding', short: 'Funding', title: 'Funding (% ann., OI-wtd)', group: 'perp', param: 'funding_venues',
    series: [{ type: 'bars' as const, field: 'value', signed: true }] },
  { key: 'spot_cvd', short: 'Spot CVD', title: 'Spot CVD', group: 'spot', param: 'spot_venues',
    series: [{ type: 'candle' as const }] },
  { key: 'fut_cvd', short: 'Fut CVD', title: 'Futures CVD', group: 'fut', param: 'fut_venues',
    series: [{ type: 'candle' as const }] },
  { key: 'liq', short: 'Liquidations', title: 'Liquidations (coins)', group: 'liq', param: 'liq_venues',
    series: [
      { type: 'bars' as const, field: 'short', color: UP, title: 'shorts liquidated' },
      { type: 'bars' as const, field: 'long', color: DOWN, negate: true, title: 'longs liquidated' },
    ] },
  { key: 'optoi', short: 'Options OI', title: 'Options OI (coins)', group: 'opt', param: 'opt_venues',
    series: [
      { type: 'line' as const, field: 'call', color: UP, title: 'calls' },
      { type: 'line' as const, field: 'put', color: DOWN, title: 'puts' },
    ] },
] as const

const CANDLE = {
  upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN,
  wickUpColor: UP, wickDownColor: DOWN,
}

// Fraction of each pane left empty above the high and below the low. The
// library's own defaults are 0.2/0.1, which wastes a fifth of a 150px pane;
// 0.06 puts the max just under the top edge and the min just over the bottom.
const FIT_MARGINS = { top: 0.06, bottom: 0.06 }

// 1..1440 minutes. 90 reads as 1.5h, which is the honest label for it.
const tfLabel = (m: number) =>
  m < 60 ? `${m}m` : m < 1440 ? `${m / 60}h` : '1d'

export default function App() {
  const box = useRef<HTMLDivElement>(null)
  const series = useRef<ISeriesApi<SeriesType>[][]>([])
  const chartApi = useRef<IChartApi | null>(null)
  const view = useRef('')
  const keepRange = useRef<{ from: number; to: number } | null>(null)
  const [paneH, setPaneH] = useState<number[]>([])
  const [markets, setMarkets] = useState<Markets | null>(null)
  const [coin, setCoin] = useState('BTC')
  const [tf, setTf] = useState(5)
  const [enabled, setEnabled] = useState<Record<string, Set<string>>>({})
  const [shown, setShown] = useState<string[]>(() => PANES.map((p) => p.key))

  // Kept in PANES order however the buttons were clicked, so a pane always
  // returns to its original slot rather than to the bottom of the stack.
  // Memoised so its identity only moves when the set does -- rebuilt every
  // render, it would tear down the chart and refetch on unrelated state.
  const panes = useMemo(() => PANES.filter((p) => shown.includes(p.key)), [shown])

  // The whole catalogue loads once; switching coin only re-picks from it.
  useEffect(() => {
    fetch(`${API}/api/markets`).then((r) => r.json()).then(setMarkets)
  }, [])

  // Every exchange starts on, and the selection resets when the coin changes --
  // carrying it over would leave venues ticked that the new coin doesn't trade.
  useEffect(() => {
    const c = markets?.coins[coin]
    if (c) setEnabled(Object.fromEntries(PANES.map((p) => [p.key, new Set(c[p.group])])))
  }, [markets, coin])

  // Chart and series are created once; changing anything only re-fetches data.
  useEffect(() => {
    const chart: IChartApi = createChart(box.current!, {
      autoSize: true,
      layout: { background: { color: BG }, textColor: TEXT, panes: { separatorColor: GRID } },
      grid: { vertLines: { color: GRID, visible: false }, horzLines: { color: GRID, visible: false } },
      timeScale: { timeVisible: true },
    })

    const candles = chart.addSeries(
      CandlestickSeries,
      { ...CANDLE, borderUpColor: '#000000', borderDownColor: '#000000' },
      0,
    )
    // One entry per pane, each holding that pane's series -- a pane can draw
    // more than one (liquidations: shorts up, longs down; options OI: calls and
    // puts). They share the pane's price scale, so both sides stay comparable.
    const rest = panes.map((pane, i) =>
      pane.series.map((sp) => {
        const type = { candle: CandlestickSeries, bars: HistogramSeries, line: LineSeries }[sp.type]
        const title = 'title' in sp ? `${pane.title} — ${sp.title}` : pane.title
        const color = 'color' in sp ? sp.color : UP
        return chart.addSeries(
          type as never,
          (sp.type === 'candle' ? { ...CANDLE, title }
            : sp.type === 'line' ? { title, color, lineWidth: 2 }
            : { title, color }) as never,
          i + 1,
        ) as ISeriesApi<SeriesType>
      }),
    )
    series.current = [[candles], ...rest]
    for (const s of series.current.flat()) {
      s.priceScale().applyOptions({ autoScale: true, scaleMargins: FIT_MARGINS })
    }
    // Price keeps its stretch, so hiding a pane hands its room to the ones left
    // rather than reflowing everything -- with all four off, price takes the lot.
    chart.panes().forEach((p, i) => p.setStretchFactor(i === 0 ? PRICE_STRETCH : PANE_STRETCH))
    chartApi.current = chart

    // The sidebar rows are pinned to the panes, but the panes share the chart
    // minus the time axis -- so read the heights back rather than assuming the
    // stretch factors in pixels, which would drift by the axis height.
    const sync = () => setPaneH(chart.panes().map((p) => p.getHeight()))
    const raf = requestAnimationFrame(sync)
    window.addEventListener('resize', sync)

    return () => {
      cancelAnimationFrame(raf)
      window.removeEventListener('resize', sync)
      // Adding or removing a pane rebuilds the chart, which would otherwise
      // throw away the pan/zoom. Hand it to the next build to restore once the
      // new series have data -- setting a range on an empty series does nothing.
      keepRange.current = chart.timeScale().getVisibleLogicalRange() ?? null
      chart.remove()
    }
  }, [panes])

  // Re-fetch on coin, timeframe or exchange change. `stale` drops a response
  // that lost the race to a newer one -- switching coins quickly would
  // otherwise leave whichever request finished last on screen.
  useEffect(() => {
    if (!markets?.coins[coin] || !Object.keys(enabled).length) return
    let stale = false
    const params = new URLSearchParams({ coin, tf: String(tf) })
    // A hidden pane asks for no exchanges, which short-circuits its whole query
    // server-side -- hiding panes makes the request cheaper, not just tidier.
    for (const p of PANES) {
      params.set(p.param, shown.includes(p.key) ? [...(enabled[p.key] ?? [])].join(',') : '')
    }

    fetch(`${API}/api/chart?${params}`)
      .then((r) => r.json())
      .then((d: Data) => {
        if (stale) return
        const [[candles], ...rest] = series.current
        candles.setData(d.ohlc as Bar[])
        panes.forEach((pane, i) =>
          pane.series.forEach((sp, j) => {
            if (sp.type === 'candle') return rest[i][j].setData(d[pane.key] as Bar[])
            const sign = 'negate' in sp && sp.negate ? -1 : 1
            const fixed = 'color' in sp ? sp.color : undefined
            rest[i][j].setData(
              (d[pane.key] as Row[]).map((r) => {
                const value = r[sp.field] * sign
                // A signed series colours per bar; a two-sided one is already
                // told apart by its own colour and its side of the zero line.
                return fixed ? { time: r.time, value, color: fixed }
                  : { time: r.time, value, color: value >= 0 ? UP : DOWN }
              }),
            )
          }),
        )
        // Re-fit every pane, not just price. Un-ticking one exchange can move a
        // pane's range by most of its height -- binance alone is ~53% of BTC
        // open interest -- and autoScale also has to be re-asserted because
        // dragging a price axis silently turns it off for that scale.
        for (const s of series.current.flat()) s.priceScale().applyOptions({ autoScale: true })
        // Horizontal fit only when the series actually changed length under it;
        // a venue or pane toggle should leave the pan/zoom where the user put it.
        const ts = chartApi.current?.timeScale()
        if (view.current !== `${coin}|${tf}`) {
          view.current = `${coin}|${tf}`
          keepRange.current = null
          ts?.fitContent()
        } else if (keepRange.current) {
          ts?.setVisibleLogicalRange(keepRange.current)
          keepRange.current = null
        }
      })
    return () => { stale = true }
  }, [markets, coin, tf, enabled, panes, shown])

  const toggle = (paneKey: string, exch: string) =>
    setEnabled((prev) => {
      const next = new Set(prev[paneKey])
      if (next.has(exch)) next.delete(exch)
      else next.add(exch)
      return { ...prev, [paneKey]: next }
    })

  const current = markets?.coins[coin]

  return (
    <>
      {/* Above the split, not inside it: the sidebar rows are pinned to the
          pane heights, so anything stacked on the chart alone would offset it. */}
      <div className="toolbar">
        <label className="field">
          Coin
          <select value={coin} onChange={(e) => setCoin(e.target.value)}>
            {Object.keys(markets?.coins ?? {}).map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </label>
        <label className="field">
          Timeframe
          <select value={tf} onChange={(e) => setTf(Number(e.target.value))}>
            {(markets?.timeframes ?? []).map((m) => (
              <option key={m} value={m}>{tfLabel(m)}</option>
            ))}
          </select>
        </label>
        <span className="field">
          Panes
          <span className="pane-toggles">
            {PANES.map((p) => (
              <button
                key={p.key}
                type="button"
                aria-pressed={shown.includes(p.key)}
                onClick={() =>
                  setShown((prev) =>
                    prev.includes(p.key) ? prev.filter((k) => k !== p.key) : [...prev, p.key],
                  )
                }
              >
                {p.short}
              </button>
            ))}
          </span>
        </span>
        {current && current.mult !== 1 && (
          <span className="note">
            quoted as {current.mult}&times;{coin} &mdash; panels rescaled to {coin}
          </span>
        )}
      </div>
      <div className="layout">
        <div ref={box} className="chart" />
        <div className="sidebar">
        <div className="sidebar-row" style={{ height: paneH[0] }} />
        {panes.map((p, i) => (
          <div className="sidebar-row" key={p.key} style={{ height: paneH[i + 1] }}>
            <div className="sidebar-title">{p.title}</div>
            {(current?.[p.group] ?? []).map((exch) => (
              <label key={exch} className="sidebar-toggle">
                <input
                  type="checkbox"
                  checked={enabled[p.key]?.has(exch) ?? false}
                  onChange={() => toggle(p.key, exch)}
                />
                {exch}
              </label>
            ))}
            {!(current?.[p.group] ?? []).length && <div className="note">no venues</div>}
          </div>
        ))}
        </div>
      </div>
    </>
  )
}
