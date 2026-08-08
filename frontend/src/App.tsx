import { useEffect, useRef, useState } from 'react'
import {
  createChart, CandlestickSeries, HistogramSeries,
  type IChartApi, type ISeriesApi, type UTCTimestamp,
} from 'lightweight-charts'
import './App.css'

// TradingView theme, same candle constants as the notebook's plotly version.
// GRID is darkened off the notebook's #E0E3EB: it was lighter than this
// background and vanished.
const BG = '#DBDBDB', GRID = '#BFBFBF', TEXT = '#131722'
const UP = '#2196F3', DOWN = '#F23645'
const API = 'http://localhost:8000'

type Bar = { time: UTCTimestamp; open: number; high: number; low: number; close: number }
type Point = { time: UTCTimestamp; value: number }
type Data = {
  ohlc: Bar[]; oi: Bar[]; funding: Point[]; spot_cvd: Bar[]; fut_cvd: Bar[]
}
type VenueLists = { perp: string[]; spot: string[] }

const PRICE_HEIGHT = 360
const PANE_HEIGHT = 150

// pane 0 is price; the rest stack under it in this order. Funding is a signed
// rate, so it reads as bars from zero; the other three are levels, so they
// candle like price does. `group` says which exchange list (perp/spot) this
// panel aggregates, `param` is the query key that selects which of them.
const PANES = [
  { key: 'oi', title: 'Open Interest', candles: true, group: 'perp', param: 'oi_venues' },
  { key: 'funding', title: 'Funding (% ann.)', candles: false, group: 'perp', param: 'funding_venues' },
  { key: 'spot_cvd', title: 'Spot CVD', candles: true, group: 'spot', param: 'spot_venues' },
  { key: 'fut_cvd', title: 'Futures CVD', candles: true, group: 'perp', param: 'fut_venues' },
] as const

const CANDLE = {
  upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN,
  wickUpColor: UP, wickDownColor: DOWN,
}

export default function App() {
  const box = useRef<HTMLDivElement>(null)
  const series = useRef<ISeriesApi<'Candlestick' | 'Histogram'>[]>([])
  const [venues, setVenues] = useState<VenueLists | null>(null)
  const [enabled, setEnabled] = useState<Record<string, Set<string>>>({})

  // Toggle-able exchanges per panel, loaded once. Every exchange starts on.
  useEffect(() => {
    fetch(`${API}/api/venues`)
      .then((r) => r.json())
      .then((v: VenueLists) => {
        setVenues(v)
        setEnabled(Object.fromEntries(PANES.map((p) => [p.key, new Set(v[p.group])])))
      })
  }, [])

  // Chart and series are created once; toggling exchanges only re-fetches data.
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
    chart.panes()[0].setHeight(PRICE_HEIGHT)

    const rest = PANES.map(({ candles: isCandle, title }, i) => {
      const s = chart.addSeries(
        isCandle ? CandlestickSeries : HistogramSeries,
        isCandle ? { ...CANDLE, title } : { title },
        i + 1,
      )
      chart.panes()[i + 1].setHeight(PANE_HEIGHT)
      return s
    })
    series.current = [candles, ...rest]

    return () => chart.remove()
  }, [])

  // Re-fetch whenever the enabled-exchange selection changes.
  useEffect(() => {
    if (!venues) return
    const params = new URLSearchParams()
    for (const p of PANES) params.set(p.param, [...(enabled[p.key] ?? [])].join(','))

    fetch(`${API}/api/chart?${params}`)
      .then((r) => r.json())
      .then((d: Data) => {
        const [candles, ...rest] = series.current
        candles.setData(d.ohlc)
        PANES.forEach(({ key, candles: isCandle }, i) =>
          rest[i].setData(
            isCandle
              ? (d[key] as Bar[])
              : (d[key] as Point[]).map((p) => ({ ...p, color: p.value >= 0 ? UP : DOWN })),
          ),
        )
        candles.priceScale().applyOptions({ autoScale: true })
      })
  }, [venues, enabled])

  const toggle = (paneKey: string, exch: string) =>
    setEnabled((prev) => {
      const next = new Set(prev[paneKey])
      if (next.has(exch)) next.delete(exch)
      else next.add(exch)
      return { ...prev, [paneKey]: next }
    })

  return (
    <div className="layout">
      <div ref={box} className="chart" />
      <div className="sidebar">
        <div className="sidebar-row" style={{ height: PRICE_HEIGHT }} />
        {PANES.map((p) => (
          <div className="sidebar-row" key={p.key} style={{ height: PANE_HEIGHT }}>
            <div className="sidebar-title">{p.title}</div>
            {(venues?.[p.group] ?? []).map((exch) => (
              <label key={exch} className="sidebar-toggle">
                <input
                  type="checkbox"
                  checked={enabled[p.key]?.has(exch) ?? false}
                  onChange={() => toggle(p.key, exch)}
                />
                {exch}
              </label>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}
