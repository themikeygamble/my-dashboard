/* ================================================================
   Sector Rotation Map — Live RRG Dashboard
   Fetches data from FastAPI backend, renders D3 scatter + tails
   ================================================================ */

const API_BASE = "https://themikeygamble-api.onrender.com"
;

/* ── State ── */
const state = {
  benchmark: "VTI",
  tailLength: 8,
  customSymbols: [],        // user-added tickers
  portfolios: [],           // user-created portfolios [{id, name, color, symbols:[]}]
  portfolioRRG: {},         // composite RRG data keyed by portfolio id
  sectors: {},              // latest API response sectors (ETFs + BTC + custom)
  holdings: {},             // sector holdings metadata from /api/holdings
  stockData: {},            // computed RRG for individual stocks, keyed by symbol
  expandedSectors: new Set(), // which sector ETFs are expanded in sidebar
  visibleStocks: new Set(),   // individual stocks toggled ON for chart display
  loadingStocks: new Set(),   // sectors currently loading stock data
  highlighted: null,
  dimmedSymbols: new Set(),
  loading: false,
  _nextPortfolioId: 1,
};

/* ── Constants ── */
const BENCHMARKS = [
  { id: "SPY", label: "SPY" },
  { id: "VTI", label: "VTI" },
  { id: "QQQ", label: "QQQ" },
  { id: "DIA", label: "DIA" },
  { id: "IWM", label: "IWM" },
  { id: "BTC-USD", label: "BTC" },
];

const TAIL_LENGTHS = [6, 8, 12, 16, 20];

const BENCHMARK_NAMES = {
  SPY: "S&P 500", VTI: "Total Market", QQQ: "Nasdaq 100",
  DIA: "Dow Jones", IWM: "Russell 2000", "BTC-USD": "Bitcoin",
  XLK: "Technology", XLF: "Financials", XLV: "Health Care",
  XLE: "Energy", XLY: "Consumer Disc.", XLP: "Consumer Staples",
  XLI: "Industrials", XLB: "Materials", XLU: "Utilities",
  XLRE: "Real Estate", XLC: "Comm. Services",
};

/* Friendly name for crypto sub-items on chart labels */
const CRYPTO_DISPLAY = {
  "ETH-USD": "ETH", "SOL-USD": "SOL", "BNB-USD": "BNB",
  "XRP-USD": "XRP", "ADA-USD": "ADA", "DOGE-USD": "DOGE",
  "TRX-USD": "TRX", "AVAX-USD": "AVAX", "LINK-USD": "LINK",
  "DOT-USD": "DOT",
};

const QUADRANT_COLORS = {
  Leading:   { bg: "rgba(0,100,0,0.12)" },
  Weakening: { bg: "rgba(120,100,0,0.10)" },
  Lagging:   { bg: "rgba(100,0,0,0.12)" },
  Improving: { bg: "rgba(0,80,80,0.12)" },
};

const DISPLAY_LABELS = { "BTC-USD": "Crypto" };
function displaySymbol(sym) {
  if (sym.startsWith("_pf_")) {
    const pf = getPortfolioByKey(sym);
    return pf ? pf.name : sym;
  }
  return DISPLAY_LABELS[sym] || sym.replace("-USD", "");
}

/* Which sectors/groups have expandable holdings */
const SECTOR_ETFS = new Set([
  "XLK","XLF","XLV","XLE","XLY","XLP","XLI","XLB","XLU","XLRE","XLC","BTC-USD"
]);

/* Sector metadata for placeholder entries when a sector is the benchmark */
const SECTOR_META = {
  XLK: { name: "Technology", color: "#00BCD4" },
  XLF: { name: "Financials", color: "#2196F3" },
  XLV: { name: "Health Care", color: "#E91E63" },
  XLE: { name: "Energy", color: "#FF5722" },
  XLY: { name: "Consumer Discretionary", color: "#FF9800" },
  XLP: { name: "Consumer Staples", color: "#8BC34A" },
  XLI: { name: "Industrials", color: "#9E9E9E" },
  XLB: { name: "Materials", color: "#795548" },
  XLU: { name: "Utilities", color: "#FFEB3B" },
  XLRE: { name: "Real Estate", color: "#009688" },
  XLC: { name: "Communication Services", color: "#9C27B0" },
  "BTC-USD": { name: "Crypto", color: "#F7931A" },
};

const PORTFOLIO_COLORS = [
  "#CE93D8", "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
  "#FFEAA7", "#DDA0DD", "#F7DC6F", "#BB8FCE", "#85C1E9",
  "#F8C471", "#82E0AA", "#F1948A", "#AED6F1", "#D2B4DE",
];

/* SVG chevron used for expand arrows */
const CHEVRON_SVG = `<svg viewBox="0 0 12 12" fill="none"><path d="M4.5 2.5L8 6L4.5 9.5" stroke="#e6edf3" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>`;

/* ── API ── */
async function fetchRRGData() {
  const extra = state.customSymbols.join(",");
  const url = `${API_BASE}/api/rrg?benchmark=${state.benchmark}&tail=${state.tailLength}${extra ? "&extra=" + extra : ""}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function fetchHoldings() {
  const res = await fetch(`${API_BASE}/api/holdings`);
  if (!res.ok) throw new Error("Failed to load holdings");
  return res.json();
}

async function fetchStockRRG(symbols) {
  const url = `${API_BASE}/api/rrg-stocks?symbols=${symbols.join(",")}&benchmark=${state.benchmark}&tail=${state.tailLength}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Stock RRG error: ${res.status}`);
  return res.json();
}

async function validateSymbol(symbol) {
  const res = await fetch(`${API_BASE}/api/validate-symbol`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ symbol }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `Invalid symbol: ${symbol}`);
  }
  return res.json();
}

async function fetchPortfolioRRG(symbols) {
  const res = await fetch(`${API_BASE}/api/rrg-portfolio`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      symbols: symbols,
      benchmark: state.benchmark,
      tail: state.tailLength,
    }),
  });
  if (!res.ok) throw new Error(`Portfolio RRG error: ${res.status}`);
  return res.json();
}

/* ── Loading ── */
function showLoading(show) {
  state.loading = show;
  let overlay = document.getElementById("loadingOverlay");
  if (show) {
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "loadingOverlay";
      overlay.className = "loading-overlay";
      overlay.innerHTML = `<div class="loading-spinner"></div><span class="loading-text">Fetching live data…</span>`;
      document.getElementById("chartContainer").appendChild(overlay);
    }
    overlay.classList.add("visible");
  } else if (overlay) {
    overlay.classList.remove("visible");
  }
}

/* ── Load Data ── */
async function loadData() {
  showLoading(true);
  try {
    const data = await fetchRRGData();
    state.sectors = data.sectors;

    // If the benchmark is a sector ETF, inject a placeholder so it stays in the sidebar
    if (SECTOR_ETFS.has(state.benchmark) && !state.sectors[state.benchmark]) {
      const meta = SECTOR_META[state.benchmark];
      state.sectors[state.benchmark] = {
        symbol: state.benchmark,
        name: meta.name,
        color: meta.color,
        isDefault: true,
        isBenchmark: true,
        tail: [],
        current: null,
        quadrant: "Benchmark",
      };
    }

    const benchLabel = BENCHMARKS.find(b => b.id === state.benchmark)?.label || displaySymbol(state.benchmark);
    const benchName = BENCHMARK_NAMES[state.benchmark] || benchLabel;
    const subtitle = SECTOR_ETFS.has(state.benchmark)
      ? `Sector Stocks vs ${benchLabel} (${benchName})`
      : `S&P 500 Sector SPDRs vs ${benchLabel}`;
    document.getElementById("siteSubtitle").textContent = subtitle;

    const dateStr = data.latest_data_date
      ? new Date(data.latest_data_date + "T00:00:00").toLocaleDateString("en-US", {
          month: "short", day: "numeric", year: "numeric",
        })
      : "—";
    document.getElementById("updatedDate").textContent = dateStr;

    // Re-fetch stock data for any expanded sectors
    const expandedPromises = [];
    for (const sectorSym of state.expandedSectors) {
      if (state.holdings[sectorSym]) {
        const syms = state.holdings[sectorSym].holdings.map(h => h.symbol);
        expandedPromises.push(
          fetchStockRRG(syms).then(result => {
            for (const [sym, data2] of Object.entries(result.stocks)) {
              state.stockData[sym] = data2;
            }
          }).catch(() => {})
        );
      }
    }

    // Re-fetch stock data for expanded portfolios
    for (const pf of state.portfolios) {
      const pfKey = `_pf_${pf.id}`;
      if (state.expandedSectors.has(pfKey)) {
        expandedPromises.push(
          fetchStockRRG(pf.symbols).then(result => {
            for (const [sym, data2] of Object.entries(result.stocks)) {
              state.stockData[sym] = data2;
            }
          }).catch(() => {})
        );
      }
    }

    await Promise.all(expandedPromises);

    // Fetch composite RRG for all portfolios (always, so they plot on the chart)
    const pfPromises = state.portfolios.map(pf =>
      fetchPortfolioRRG(pf.symbols)
        .then(result => {
          state.portfolioRRG[pf.id] = result.portfolio;
        })
        .catch(err => {
          console.error(`Failed to load portfolio RRG for ${pf.name}:`, err);
          state.portfolioRRG[pf.id] = null;
        })
    );
    await Promise.all(pfPromises);

    renderChart();
    renderSidebar();
  } catch (err) {
    console.error("Failed to load RRG data:", err);
  } finally {
    showLoading(false);
  }
}

/* ── Toggle sector expansion ── */
async function toggleSectorExpand(sectorSym) {
  if (state.expandedSectors.has(sectorSym)) {
    state.expandedSectors.delete(sectorSym);
    // Hide all stocks in this sector
    const holdingSyms = getHoldingSymbols(sectorSym);
    holdingSyms.forEach(s => state.visibleStocks.delete(s));
    // If benchmark is this sector, revert to default
    if (state.benchmark === sectorSym) {
      switchBenchmark(DEFAULT_BENCHMARK);
    } else {
      renderChart();
      renderSidebar();
    }
    return;
  }

  state.expandedSectors.add(sectorSym);

  // For portfolios, fetch stock data
  const pf = getPortfolioByKey(sectorSym);
  if (pf) {
    const needFetch = pf.symbols.filter(s => !state.stockData[s]);
    if (needFetch.length > 0) {
      state.loadingStocks.add(sectorSym);
      renderSidebar();
      try {
        const result = await fetchStockRRG(pf.symbols);
        for (const [sym, data] of Object.entries(result.stocks)) {
          state.stockData[sym] = data;
        }
      } catch (err) {
        console.error(`Failed to load stocks for portfolio ${pf.name}:`, err);
      } finally {
        state.loadingStocks.delete(sectorSym);
      }
    }
    renderSidebar();
    return;
  }

  // For regular sectors
  const sectorHoldings = state.holdings[sectorSym];
  if (!sectorHoldings) { renderSidebar(); return; }

  const syms = sectorHoldings.holdings.map(h => h.symbol);
  const needFetch = syms.filter(s => !state.stockData[s]);

  if (needFetch.length > 0) {
    state.loadingStocks.add(sectorSym);
    renderSidebar();
    try {
      const result = await fetchStockRRG(syms);
      for (const [sym, data] of Object.entries(result.stocks)) {
        state.stockData[sym] = data;
      }
    } catch (err) {
      console.error(`Failed to load stocks for ${sectorSym}:`, err);
    } finally {
      state.loadingStocks.delete(sectorSym);
    }
  }

  renderSidebar();
}

/* ── Default benchmark ── */
const DEFAULT_BENCHMARK = "VTI";

/* ── Get holding symbols for a sector key (including portfolios) ── */
function getHoldingSymbols(sectorKey) {
  const pf = getPortfolioByKey(sectorKey);
  if (pf) return pf.symbols;
  return (state.holdings[sectorKey]?.holdings || []).map(h => h.symbol);
}

/* ── Get portfolio by its sidebar key ── */
function getPortfolioByKey(key) {
  if (!key.startsWith("_pf_")) return null;
  const id = parseInt(key.replace("_pf_", ""), 10);
  return state.portfolios.find(p => p.id === id) || null;
}

/* ── Toggle all stocks in a sector on/off ── */
function toggleSectorStocks(sectorKey) {
  const holdingSyms = getHoldingSymbols(sectorKey);
  const allVisible = holdingSyms.every(s => state.visibleStocks.has(s));

  if (allVisible) {
    holdingSyms.forEach(s => state.visibleStocks.delete(s));
    // For non-portfolio sectors, revert benchmark if needed
    if (!sectorKey.startsWith("_pf_") && state.benchmark === sectorKey) {
      switchBenchmark(DEFAULT_BENCHMARK);
    } else {
      renderChart();
      renderSidebar();
    }
  } else {
    holdingSyms.forEach(s => state.visibleStocks.add(s));
    // Auto-switch benchmark for real sectors
    if (SECTOR_ETFS.has(sectorKey)) {
      switchBenchmark(sectorKey);
    } else {
      renderChart();
      renderSidebar();
    }
  }
}

/* ── Toggle individual stock ── */
function toggleStock(sym) {
  if (state.visibleStocks.has(sym)) {
    state.visibleStocks.delete(sym);
    // If no stocks remain visible for the current benchmark sector, revert
    const parentSector = findParentSector(sym);
    if (parentSector && state.benchmark === parentSector) {
      const sectorStocks = getHoldingSymbols(parentSector);
      const anyStillVisible = sectorStocks.some(s => state.visibleStocks.has(s));
      if (!anyStillVisible) {
        switchBenchmark(DEFAULT_BENCHMARK);
        return;
      }
    }
    renderChart();
    renderSidebar();
  } else {
    state.visibleStocks.add(sym);
    // Find the parent sector and auto-switch benchmark to it
    const parentSector = findParentSector(sym);
    if (parentSector && SECTOR_ETFS.has(parentSector)) {
      switchBenchmark(parentSector);
    } else {
      renderChart();
      renderSidebar();
    }
  }
}

/* ── Find which sector ETF a stock belongs to ── */
function findParentSector(sym) {
  for (const [sectorSym, meta] of Object.entries(state.holdings)) {
    if (meta.holdings.some(h => h.symbol === sym)) return sectorSym;
  }
  return null;
}

/* ── Switch benchmark (with full data reload) ── */
function switchBenchmark(newBenchmark) {
  if (newBenchmark === state.benchmark) {
    renderChart();
    renderSidebar();
    return;
  }
  state.benchmark = newBenchmark;
  state.stockData = {};
  renderBenchmarkButtons();
  loadData();
}

/* ── Controls: Benchmark ── */
function getActiveBenchmarks() {
  const ids = new Set(BENCHMARKS.map(b => b.id));
  if (!ids.has(state.benchmark) && SECTOR_ETFS.has(state.benchmark)) {
    ids.add(state.benchmark);
  }
  const list = BENCHMARKS.map(b => ({ ...b }));
  if (!BENCHMARKS.some(b => b.id === state.benchmark) && SECTOR_ETFS.has(state.benchmark)) {
    list.push({ id: state.benchmark, label: state.benchmark });
  }
  return list;
}

function renderBenchmarkButtons() {
  const container = document.getElementById("benchmarkButtons");
  container.innerHTML = "";
  const benchmarks = getActiveBenchmarks();
  benchmarks.forEach(b => {
    const btn = document.createElement("button");
    btn.className = "btn-bench" + (b.id === state.benchmark ? " active" : "");
    btn.textContent = b.label;
    btn.addEventListener("click", () => {
      if (b.id === state.benchmark) return;
      switchBenchmark(b.id);
    });
    container.appendChild(btn);
  });
}

/* ── Controls: Tail ── */
function renderTailButtons() {
  const container = document.getElementById("tailButtons");
  container.innerHTML = "";
  TAIL_LENGTHS.forEach(t => {
    const btn = document.createElement("button");
    btn.className = "btn-tail" + (t === state.tailLength ? " active" : "");
    btn.textContent = t + "w";
    btn.addEventListener("click", () => {
      if (t === state.tailLength) return;
      state.tailLength = t;
      state.stockData = {};
      renderTailButtons();
      loadData();
    });
    container.appendChild(btn);
  });
}

/* ── Highlight ── */
function setHighlight(symbol) {
  state.highlighted = symbol;
  document.querySelectorAll(".sector-row, .stock-row").forEach(row => {
    const sym = row.dataset.symbol;
    if (!symbol) {
      row.classList.remove("highlighted", "dimmed");
      if (state.dimmedSymbols.has(sym)) row.classList.add("dimmed");
    } else {
      row.classList.toggle("highlighted", sym === symbol);
      row.classList.toggle("dimmed", sym !== symbol);
    }
  });
  updateChartHighlight(symbol);
}

/* ── Chart ── */
let svg, xScale, yScale, chartG;
const margin = { top: 40, right: 40, bottom: 50, left: 60 };

function getVisibleItems() {
  const items = [];

  // Sector ETFs + BTC + custom symbols (skip benchmark placeholder — no tail data)
  Object.values(state.sectors).forEach(sec => {
    if (sec.isBenchmark) return;
    if (!state.dimmedSymbols.has(sec.symbol)) {
      items.push({ ...sec, isStock: false });
    }
  });

  // Portfolio composites (plotted as sector-level items, not stocks)
  state.portfolios.forEach(pf => {
    const pfKey = `_pf_${pf.id}`;
    const rrg = state.portfolioRRG[pf.id];
    if (!rrg || !rrg.tail || rrg.tail.length === 0) return;
    if (state.dimmedSymbols.has(pfKey)) return;
    items.push({
      symbol: pfKey,
      name: pf.name,
      color: pf.color,
      tail: rrg.tail,
      current: rrg.current,
      quadrant: rrg.quadrant,
      isStock: false,
      isPortfolio: true,
    });
  });

  // Individual stocks that are toggled visible
  state.visibleStocks.forEach(sym => {
    const stockInfo = state.stockData[sym];
    if (!stockInfo) return;
    // Find which sector this stock belongs to and inherit color
    let color = "#aaaaaa";
    for (const [sectorSym, sectorMeta] of Object.entries(state.holdings)) {
      const match = sectorMeta.holdings.find(h => h.symbol === sym);
      if (match) {
        const sectorData = state.sectors[sectorSym];
        color = sectorData?.color || "#aaaaaa";
        break;
      }
    }
    // Check portfolios for color
    for (const pf of state.portfolios) {
      if (pf.symbols.includes(sym)) {
        color = pf.color;
        break;
      }
    }
    items.push({
      symbol: sym,
      name: stockInfo.name || sym,
      color: color,
      tail: stockInfo.tail,
      current: stockInfo.current,
      quadrant: stockInfo.quadrant,
      isStock: true,
    });
  });

  return items;
}

function getAllDomainItems() {
  const items = [];
  Object.values(state.sectors).forEach(sec => {
    if (sec.isBenchmark) return;
    sec.tail.forEach(pt => items.push(pt));
  });
  // Portfolio composites
  state.portfolios.forEach(pf => {
    const rrg = state.portfolioRRG[pf.id];
    if (rrg && rrg.tail) rrg.tail.forEach(pt => items.push(pt));
  });
  state.visibleStocks.forEach(sym => {
    const s = state.stockData[sym];
    if (s) s.tail.forEach(pt => items.push(pt));
  });
  return items;
}

function renderChart() {
  const wrapper = document.getElementById("chartWrapper");
  const rect = wrapper.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;

  wrapper.innerHTML = "";

  svg = d3.select(wrapper).append("svg").attr("width", width).attr("height", height);
  chartG = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  // Domain from ALL points (stable grid)
  const allPts = getAllDomainItems();
  if (allPts.length === 0) return;

  const xMin = d3.min(allPts, d => d.rs_ratio);
  const xMax = d3.max(allPts, d => d.rs_ratio);
  const yMin = d3.min(allPts, d => d.rs_momentum);
  const yMax = d3.max(allPts, d => d.rs_momentum);
  const xPad = Math.max((xMax - xMin) * 0.15, 1);
  const yPad = Math.max((yMax - yMin) * 0.15, 1);

  xScale = d3.scaleLinear()
    .domain([Math.min(xMin - xPad, 98), Math.max(xMax + xPad, 102)])
    .range([0, innerW]);
  yScale = d3.scaleLinear()
    .domain([Math.min(yMin - yPad, 98), Math.max(yMax + yPad, 102)])
    .range([innerH, 0]);

  const cx = xScale(100), cy = yScale(100);

  // Quadrants
  const quads = [
    { x: cx, y: 0,  w: innerW - cx, h: cy,          key: "Leading" },
    { x: 0,  y: 0,  w: cx,          h: cy,          key: "Improving" },
    { x: cx, y: cy, w: innerW - cx, h: innerH - cy, key: "Weakening" },
    { x: 0,  y: cy, w: cx,          h: innerH - cy, key: "Lagging" },
  ];
  quads.forEach(q => {
    if (q.w > 0 && q.h > 0) {
      chartG.append("rect").attr("x", q.x).attr("y", q.y)
        .attr("width", q.w).attr("height", q.h)
        .attr("fill", QUADRANT_COLORS[q.key].bg);
      chartG.append("text").attr("class", "quadrant-label")
        .attr("x", q.x + q.w / 2).attr("y", q.y + q.h / 2)
        .attr("text-anchor", "middle").attr("dominant-baseline", "middle")
        .text(q.key);
    }
  });

  // Crosshairs
  chartG.append("line").attr("class", "crosshair-line")
    .attr("x1", cx).attr("x2", cx).attr("y1", 0).attr("y2", innerH);
  chartG.append("line").attr("class", "crosshair-line")
    .attr("x1", 0).attr("x2", innerW).attr("y1", cy).attr("y2", cy);

  // Grid
  chartG.append("g").attr("class", "grid").attr("transform", `translate(0,${innerH})`)
    .call(d3.axisBottom(xScale).ticks(8).tickSize(-innerH)).selectAll("text").remove();
  chartG.append("g").attr("class", "grid")
    .call(d3.axisLeft(yScale).ticks(8).tickSize(-innerW)).selectAll("text").remove();

  // Axes
  chartG.append("g").attr("class", "axis x-axis").attr("transform", `translate(0,${innerH})`)
    .call(d3.axisBottom(xScale).ticks(8));
  chartG.append("g").attr("class", "axis y-axis").call(d3.axisLeft(yScale).ticks(8));

  // Axis labels
  chartG.append("text").attr("class", "axis-label")
    .attr("x", innerW / 2).attr("y", innerH + 40)
    .attr("text-anchor", "middle").text("RS-Ratio →");
  chartG.append("text").attr("class", "axis-label")
    .attr("x", -innerH / 2).attr("y", -45)
    .attr("text-anchor", "middle").attr("transform", "rotate(-90)").text("RS-Momentum →");

  // Draw tails
  const sectorGroup = chartG.append("g").attr("class", "sectors-group");
  const lineGen = d3.line()
    .x(d => xScale(d.rs_ratio)).y(d => yScale(d.rs_momentum))
    .curve(d3.curveCardinal.tension(0.4));

  const visibleItems = getVisibleItems();

  visibleItems.forEach(item => {
    const g = sectorGroup.append("g")
      .attr("class", "sector-g")
      .attr("data-symbol", item.symbol);

    const isStock = item.isStock;
    const tailOpacity = isStock ? 0.45 : 0.6;
    const strokeWidth = isStock ? 1.5 : 2;

    if (item.tail.length > 1) {
      g.append("path").attr("class", "sector-tail")
        .attr("d", lineGen(item.tail))
        .attr("stroke", item.color)
        .attr("stroke-width", strokeWidth)
        .attr("opacity", tailOpacity);
    }

    item.tail.forEach((pt, i) => {
      const isLast = i === item.tail.length - 1;
      const progress = item.tail.length > 1 ? i / (item.tail.length - 1) : 1;
      const baseR = isStock ? 2 : 2.5;
      const radius = isLast ? (isStock ? 4.5 : 6) : baseR + progress * 2;
      const opacity = isLast ? 1 : 0.3 + progress * 0.5;

      const circle = g.append("circle")
        .attr("class", isLast ? "tail-point current-point" : "tail-point")
        .attr("cx", xScale(pt.rs_ratio)).attr("cy", yScale(pt.rs_momentum))
        .attr("r", radius).attr("fill", item.color)
        .attr("opacity", opacity).style("color", item.color);

      if (isStock && isLast) {
        g.append("circle")
          .attr("cx", xScale(pt.rs_ratio)).attr("cy", yScale(pt.rs_momentum))
          .attr("r", 6).attr("fill", "none")
          .attr("stroke", item.color).attr("stroke-width", 1)
          .attr("stroke-dasharray", "2 2").attr("opacity", 0.5);
      }

      if (isLast) {
        g.append("text").attr("class", "point-label")
          .attr("x", xScale(pt.rs_ratio) + 9)
          .attr("y", yScale(pt.rs_momentum) + 4)
          .attr("fill", "#ffffff").attr("font-size", isStock ? "10px" : "11px")
          .attr("font-weight", isStock ? "600" : "700")
          .attr("font-family", "'JetBrains Mono', monospace")
          .text(displaySymbol(item.symbol));
      }

      circle
        .on("mouseenter", (event) => { showTooltip(event, item, pt, i); setHighlight(item.symbol); })
        .on("mousemove", (event) => moveTooltip(event))
        .on("mouseleave", () => { hideTooltip(); setHighlight(null); });
    });
  });
}

function updateChartHighlight(symbol) {
  if (!chartG) return;
  chartG.selectAll(".sector-g").each(function () {
    const g = d3.select(this);
    const sym = g.attr("data-symbol");
    if (!symbol) {
      g.attr("opacity", state.dimmedSymbols.has(sym) ? 0.15 : 1);
    } else {
      g.attr("opacity", sym === symbol ? 1 : 0.12);
    }
  });
}

/* ── Tooltip ── */
const tooltip = document.getElementById("tooltip");

function showTooltip(event, item, point, idx) {
  const isLatest = idx === item.tail.length - 1;
  tooltip.innerHTML = `
    <div class="tooltip-header">
      <span class="tooltip-dot" style="background:${item.color}"></span>
      <span class="tooltip-symbol">${displaySymbol(item.symbol)}</span>
      <span class="tooltip-name">${item.name}</span>
    </div>
    <div class="tooltip-row"><span class="tooltip-label">RS-Ratio</span><span class="tooltip-value">${point.rs_ratio.toFixed(2)}</span></div>
    <div class="tooltip-row"><span class="tooltip-label">RS-Momentum</span><span class="tooltip-value">${point.rs_momentum.toFixed(2)}</span></div>
    <div class="tooltip-row"><span class="tooltip-label">Quadrant</span><span class="tooltip-value">${item.quadrant}</span></div>
    <div class="tooltip-row"><span class="tooltip-label">Date</span><span class="tooltip-value">${point.date}</span></div>
    ${!isLatest ? `<div class="tooltip-row"><span class="tooltip-label" style="color:var(--text-faint);font-style:italic">Week ${idx + 1} of ${item.tail.length}</span></div>` : ""}
  `;
  tooltip.classList.add("visible");
  moveTooltip(event);
}

function moveTooltip(event) {
  const pad = 14;
  let x = event.clientX + pad, y = event.clientY + pad;
  const r = tooltip.getBoundingClientRect();
  if (x + r.width > window.innerWidth) x = event.clientX - r.width - pad;
  if (y + r.height > window.innerHeight) y = event.clientY - r.height - pad;
  tooltip.style.left = x + "px";
  tooltip.style.top = y + "px";
}

function hideTooltip() { tooltip.classList.remove("visible"); }

/* ── Sidebar ── */
function renderSidebar() {
  const list = document.getElementById("sectorList");
  list.innerHTML = "";

  const sectors = Object.values(state.sectors);
  const defaults = sectors.filter(s => s.isDefault).sort((a, b) => a.name.localeCompare(b.name));
  const customs = sectors.filter(s => !s.isDefault).sort((a, b) => a.symbol.localeCompare(b.symbol));
  const sorted = [...defaults, ...customs];

  // Update count
  const countEl = document.querySelector(".sector-count");
  const stockCount = state.visibleStocks.size;
  const pfCount = state.portfolios.length;
  if (countEl) {
    let txt = `${sorted.length} Tracked`;
    if (pfCount > 0) txt += ` + ${pfCount} Portfolio${pfCount > 1 ? "s" : ""}`;
    if (stockCount > 0) txt += ` + ${stockCount} Stocks`;
    countEl.textContent = txt;
  }

  // Render default + custom sectors
  sorted.forEach(sec => {
    renderSectorRow(list, sec);
  });

  // Render portfolios
  state.portfolios.forEach(pf => {
    renderPortfolioRow(list, pf);
  });
}

function renderSectorRow(list, sec) {
  const hasSubs = SECTOR_ETFS.has(sec.symbol) && state.holdings[sec.symbol];
  const isExpanded = state.expandedSectors.has(sec.symbol);
  const isLoading = state.loadingStocks.has(sec.symbol);

  const row = document.createElement("div");
  row.className = "sector-row" + (state.dimmedSymbols.has(sec.symbol) ? " dimmed" : "");
  row.dataset.symbol = sec.symbol;

  const qClass = sec.quadrant.toLowerCase();
  const rsR = sec.current ? sec.current.rs_ratio.toFixed(2) : "—";
  const rsM = sec.current ? sec.current.rs_momentum.toFixed(2) : "—";

  const expandIcon = hasSubs
    ? `<span class="expand-icon ${isExpanded ? "expanded" : ""}">${CHEVRON_SVG}</span>`
    : `<span class="expand-icon-placeholder"></span>`;

  const removeBtn = !sec.isDefault
    ? `<button class="btn-remove-symbol" data-symbol="${sec.symbol}" title="Remove ${displaySymbol(sec.symbol)}">&#10005;</button>`
    : "";

  row.innerHTML = `
    ${expandIcon}
    <span class="sector-dot" style="background:${sec.color}"></span>
    <div class="sector-info">
      <div class="sector-symbol">${displaySymbol(sec.symbol)}${removeBtn}</div>
      <div class="sector-name">${sec.name}</div>
    </div>
    <div class="sector-meta">
      <span class="quadrant-badge ${qClass}">${sec.quadrant}</span>
      <span class="sector-values">${rsR} / ${rsM}</span>
    </div>
  `;

  // Click toggles visibility
  if (!sec.isBenchmark) {
    row.addEventListener("click", (e) => {
      if (e.target.closest(".expand-icon") || e.target.closest(".btn-remove-symbol") || e.target.closest(".btn-sector-toggle")) return;
      if (state.dimmedSymbols.has(sec.symbol)) {
        state.dimmedSymbols.delete(sec.symbol);
      } else {
        state.dimmedSymbols.add(sec.symbol);
      }
      renderChart();
      renderSidebar();
    });
  }

  // Expand icon click
  const expEl = row.querySelector(".expand-icon");
  if (expEl) {
    expEl.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleSectorExpand(sec.symbol);
    });
  }

  if (!sec.isBenchmark) {
    row.addEventListener("mouseenter", () => setHighlight(sec.symbol));
    row.addEventListener("mouseleave", () => setHighlight(null));
  }
  list.appendChild(row);

  // Remove button
  const rmBtn = row.querySelector(".btn-remove-symbol");
  if (rmBtn) {
    rmBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      state.customSymbols = state.customSymbols.filter(s => s !== sec.symbol);
      state.dimmedSymbols.delete(sec.symbol);
      loadData();
    });
  }

  // Expanded stock sub-rows
  if (isExpanded && hasSubs) {
    const holdingsData = state.holdings[sec.symbol].holdings;
    const holdingSyms = holdingsData.map(h => h.symbol);
    const allVisible = holdingSyms.every(s => state.visibleStocks.has(s));

    // Toggle all row
    const toggleRow = document.createElement("div");
    toggleRow.className = "stock-toggle-row";
    toggleRow.innerHTML = `
      <button class="btn-sector-toggle ${allVisible ? "active" : ""}">
        ${allVisible ? "Hide All" : "Show All"}
      </button>
      ${isLoading ? '<span class="stock-loading">Loading…</span>' : ""}
    `;
    toggleRow.querySelector(".btn-sector-toggle").addEventListener("click", () => {
      toggleSectorStocks(sec.symbol);
    });
    list.appendChild(toggleRow);

    // Individual stock rows
    holdingsData.forEach(h => {
      renderStockRow(list, h, sec.symbol);
    });
  }
}

function renderPortfolioRow(list, pf) {
  const pfKey = `_pf_${pf.id}`;
  const isExpanded = state.expandedSectors.has(pfKey);
  const isLoading = state.loadingStocks.has(pfKey);
  const rrg = state.portfolioRRG[pf.id];

  const row = document.createElement("div");
  row.className = "sector-row" + (state.dimmedSymbols.has(pfKey) ? " dimmed" : "");
  row.dataset.symbol = pfKey;

  const expandIcon = `<span class="expand-icon ${isExpanded ? "expanded" : ""}">${CHEVRON_SVG}</span>`;

  const qClass = rrg?.quadrant ? rrg.quadrant.toLowerCase() : "";
  const rsR = rrg?.current ? rrg.current.rs_ratio.toFixed(2) : "—";
  const rsM = rrg?.current ? rrg.current.rs_momentum.toFixed(2) : "—";
  const quadrantLabel = rrg?.quadrant || "—";

  row.innerHTML = `
    ${expandIcon}
    <span class="sector-dot" style="background:${pf.color}"></span>
    <div class="sector-info">
      <div class="sector-symbol">${pf.name}<span class="portfolio-badge">Portfolio</span><button class="btn-edit-portfolio" title="Edit portfolio"><svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/></svg></button><button class="btn-remove-symbol" title="Remove portfolio">&#10005;</button></div>
      <div class="sector-name">${pf.symbols.length} stock${pf.symbols.length !== 1 ? "s" : ""}: ${pf.symbols.join(", ")}</div>
    </div>
    <div class="sector-meta">
      ${qClass ? `<span class="quadrant-badge ${qClass}">${quadrantLabel}</span>` : ""}
      <span class="sector-values">${rsR} / ${rsM}</span>
    </div>
  `;

  // Click toggles visibility on chart (dim/undim the portfolio composite)
  row.addEventListener("click", (e) => {
    if (e.target.closest(".expand-icon") || e.target.closest(".btn-remove-symbol") || e.target.closest(".btn-edit-portfolio")) return;
    if (state.dimmedSymbols.has(pfKey)) {
      state.dimmedSymbols.delete(pfKey);
    } else {
      state.dimmedSymbols.add(pfKey);
    }
    renderChart();
    renderSidebar();
  });

  // Expand
  const expEl = row.querySelector(".expand-icon");
  expEl.addEventListener("click", (e) => {
    e.stopPropagation();
    toggleSectorExpand(pfKey);
  });

  // Hover highlight
  row.addEventListener("mouseenter", () => setHighlight(pfKey));
  row.addEventListener("mouseleave", () => setHighlight(null));

  // Edit portfolio
  row.querySelector(".btn-edit-portfolio").addEventListener("click", (e) => {
    e.stopPropagation();
    openEditPortfolioModal(pf.id);
  });

  // Remove portfolio
  row.querySelector(".btn-remove-symbol").addEventListener("click", (e) => {
    e.stopPropagation();
    pf.symbols.forEach(s => state.visibleStocks.delete(s));
    state.expandedSectors.delete(pfKey);
    state.dimmedSymbols.delete(pfKey);
    delete state.portfolioRRG[pf.id];
    state.portfolios = state.portfolios.filter(p => p.id !== pf.id);
    renderChart();
    renderSidebar();
  });

  list.appendChild(row);

  // Expanded stock sub-rows
  if (isExpanded) {
    const holdingSyms = pf.symbols;
    const allVisible = holdingSyms.every(s => state.visibleStocks.has(s));

    const toggleRow = document.createElement("div");
    toggleRow.className = "stock-toggle-row";
    toggleRow.innerHTML = `
      <button class="btn-sector-toggle ${allVisible ? "active" : ""}">
        ${allVisible ? "Hide All" : "Show All"}
      </button>
      ${isLoading ? '<span class="stock-loading">Loading…</span>' : ""}
    `;
    toggleRow.querySelector(".btn-sector-toggle").addEventListener("click", () => {
      toggleSectorStocks(pfKey);
    });
    list.appendChild(toggleRow);

    holdingSyms.forEach(sym => {
      const stockRRG = state.stockData[sym];
      const isVisible = state.visibleStocks.has(sym);

      const sRow = document.createElement("div");
      sRow.className = "stock-row" + (isVisible ? " visible" : "");
      sRow.dataset.symbol = sym;

      const sQuadrant = stockRRG?.quadrant || "—";
      const sqClass = stockRRG ? sQuadrant.toLowerCase() : "";
      const sRsR = stockRRG?.current ? stockRRG.current.rs_ratio.toFixed(2) : "—";
      const sRsM = stockRRG?.current ? stockRRG.current.rs_momentum.toFixed(2) : "—";

      sRow.innerHTML = `
        <span class="stock-check ${isVisible ? "checked" : ""}">
          ${isVisible ? "&#10003;" : ""}
        </span>
        <div class="stock-info">
          <span class="stock-symbol">${displaySymbol(sym)}</span>
          <span class="stock-name">${stockRRG?.name || sym}</span>
        </div>
        <div class="stock-meta">
          ${stockRRG ? `<span class="quadrant-badge sm ${sqClass}">${sQuadrant}</span>` : ""}
          <span class="stock-values">${sRsR} / ${sRsM}</span>
        </div>
      `;

      sRow.addEventListener("click", () => toggleStock(sym));
      sRow.addEventListener("mouseenter", () => { if (isVisible) setHighlight(sym); });
      sRow.addEventListener("mouseleave", () => setHighlight(null));
      list.appendChild(sRow);
    });
  }
}

function renderStockRow(list, h, sectorSym) {
  const stockRRG = state.stockData[h.symbol];
  const isVisible = state.visibleStocks.has(h.symbol);

  const sRow = document.createElement("div");
  sRow.className = "stock-row" + (isVisible ? " visible" : "");
  sRow.dataset.symbol = h.symbol;

  const sQuadrant = stockRRG?.quadrant || "—";
  const sqClass = stockRRG ? sQuadrant.toLowerCase() : "";
  const sRsR = stockRRG?.current ? stockRRG.current.rs_ratio.toFixed(2) : "—";
  const sRsM = stockRRG?.current ? stockRRG.current.rs_momentum.toFixed(2) : "—";

  sRow.innerHTML = `
    <span class="stock-check ${isVisible ? "checked" : ""}">
      ${isVisible ? "&#10003;" : ""}
    </span>
    <div class="stock-info">
      <span class="stock-symbol">${displaySymbol(h.symbol)}</span>
      <span class="stock-name">${h.name}</span>
      <span class="stock-weight">${h.weight}%</span>
    </div>
    <div class="stock-meta">
      ${stockRRG ? `<span class="quadrant-badge sm ${sqClass}">${sQuadrant}</span>` : ""}
      <span class="stock-values">${sRsR} / ${sRsM}</span>
    </div>
  `;

  sRow.addEventListener("click", () => toggleStock(h.symbol));
  sRow.addEventListener("mouseenter", () => { if (isVisible) setHighlight(h.symbol); });
  sRow.addEventListener("mouseleave", () => setHighlight(null));
  list.appendChild(sRow);
}

/* ── Add Symbol Modal ── */
let addModalChips = []; // {symbol, status: 'validating'|'valid'|'invalid'}

function openAddSymbolModal() {
  editingPortfolioId = null;
  const modal = document.getElementById("addSymbolModal");
  modal.classList.add("visible");
  // Reset to symbols tab
  switchModalTab("symbols");
  // Clear symbol tab state
  document.getElementById("addSymbolInput").value = "";
  document.getElementById("addSymbolError").textContent = "";
  addModalChips = [];
  renderSymbolChips();
  document.getElementById("addSymbolInput").focus();
  // Clear portfolio tab state
  document.getElementById("portfolioNameInput").value = "";
  document.getElementById("portfolioStocksInput").value = "";
  document.getElementById("portfolioError").textContent = "";
  portfolioChips = [];
  renderPortfolioChips();
  initColorPicker();
  // Reset submit button text
  document.getElementById("portfolioSubmit").textContent = "Create Portfolio";
}

function closeAddSymbolModal() {
  document.getElementById("addSymbolModal").classList.remove("visible");
}

function switchModalTab(tabId) {
  document.querySelectorAll(".modal-tab").forEach(t => t.classList.toggle("active", t.dataset.tab === tabId));
  document.querySelectorAll(".modal-tab-content").forEach(c => c.classList.remove("active"));
  document.getElementById(tabId === "symbols" ? "tabSymbols" : "tabPortfolio").classList.add("active");
}

function renderSymbolChips() {
  const area = document.getElementById("symbolChipArea");
  area.innerHTML = "";
  addModalChips.forEach((chip, idx) => {
    const el = document.createElement("span");
    el.className = `symbol-chip ${chip.status}`;
    el.innerHTML = `${chip.symbol}<button class="chip-remove" data-idx="${idx}">&#10005;</button>`;
    el.querySelector(".chip-remove").addEventListener("click", () => {
      addModalChips.splice(idx, 1);
      renderSymbolChips();
    });
    area.appendChild(el);
  });
}

async function handleAddSymbol() {
  const input = document.getElementById("addSymbolInput");
  const errEl = document.getElementById("addSymbolError");
  const btn = document.getElementById("addSymbolSubmit");
  const raw = input.value.trim().toUpperCase();

  if (!raw) { errEl.textContent = "Please enter one or more ticker symbols"; return; }

  // Parse comma-separated symbols
  const symbols = raw.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
  if (symbols.length === 0) { errEl.textContent = "Please enter valid ticker symbols"; return; }

  errEl.textContent = "";
  input.value = "";

  // Add chips in validating state
  const newChips = [];
  for (const sym of symbols) {
    if (state.sectors[sym] || state.customSymbols.includes(sym) || addModalChips.some(c => c.symbol === sym)) {
      // Already exists or already being validated
      continue;
    }
    if (sym === state.benchmark) continue;
    const chip = { symbol: sym, status: "validating" };
    addModalChips.push(chip);
    newChips.push(chip);
  }
  renderSymbolChips();

  if (newChips.length === 0) {
    errEl.textContent = "All symbols already added or on the map";
    return;
  }

  // Validate all in parallel
  btn.disabled = true;
  btn.textContent = "Validating…";

  const results = await Promise.allSettled(
    newChips.map(chip =>
      validateSymbol(chip.symbol)
        .then(() => { chip.status = "valid"; })
        .catch(() => { chip.status = "invalid"; })
    )
  );

  renderSymbolChips();

  const validSymbols = addModalChips.filter(c => c.status === "valid").map(c => c.symbol);
  const invalidCount = addModalChips.filter(c => c.status === "invalid").length;

  if (validSymbols.length > 0) {
    // Add all valid symbols at once
    validSymbols.forEach(sym => {
      if (!state.customSymbols.includes(sym)) {
        state.customSymbols.push(sym);
      }
    });

    if (invalidCount === 0) {
      closeAddSymbolModal();
      loadData();
    } else {
      errEl.textContent = `${invalidCount} symbol${invalidCount > 1 ? "s" : ""} could not be found. Valid ones have been added.`;
      // Remove valid chips, keep invalid for user to see
      addModalChips = addModalChips.filter(c => c.status !== "valid");
      renderSymbolChips();
      loadData();
    }
  } else if (invalidCount > 0) {
    errEl.textContent = `None of the symbols could be found`;
  }

  btn.disabled = false;
  btn.textContent = "Add";
}

/* ── Portfolio Creation / Editing ── */
let portfolioChips = [];
let selectedPortfolioColor = PORTFOLIO_COLORS[0];
let editingPortfolioId = null; // null = create mode, number = edit mode

function initColorPicker() {
  const row = document.getElementById("colorPickerRow");
  row.innerHTML = "";
  PORTFOLIO_COLORS.slice(0, 10).forEach(color => {
    const swatch = document.createElement("button");
    swatch.className = "color-swatch" + (color === selectedPortfolioColor ? " selected" : "");
    swatch.style.background = color;
    swatch.addEventListener("click", () => {
      selectedPortfolioColor = color;
      row.querySelectorAll(".color-swatch").forEach(s => s.classList.remove("selected"));
      swatch.classList.add("selected");
    });
    row.appendChild(swatch);
  });
}

function renderPortfolioChips() {
  const area = document.getElementById("portfolioChipArea");
  area.innerHTML = "";
  portfolioChips.forEach((chip, idx) => {
    const el = document.createElement("span");
    el.className = `symbol-chip ${chip.status}`;
    el.innerHTML = `${chip.symbol}<button class="chip-remove" data-idx="${idx}">&#10005;</button>`;
    el.querySelector(".chip-remove").addEventListener("click", () => {
      portfolioChips.splice(idx, 1);
      renderPortfolioChips();
    });
    area.appendChild(el);
  });
}

/* Add stocks to the chip list from the input field (shared by create & edit) */
async function handleAddPortfolioStocks() {
  const stocksInput = document.getElementById("portfolioStocksInput");
  const errEl = document.getElementById("portfolioError");
  const btn = document.getElementById("portfolioAddStocksBtn");
  const raw = stocksInput.value.trim().toUpperCase();

  if (!raw) { errEl.textContent = "Please enter one or more ticker symbols"; return; }

  const symbols = raw.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
  if (symbols.length === 0) { errEl.textContent = "Please enter valid ticker symbols"; return; }

  errEl.textContent = "";
  stocksInput.value = "";

  // Duplicate protection — skip symbols already in the chip list
  const existingSyms = new Set(portfolioChips.map(c => c.symbol));
  const dupes = [];
  const newSymbols = [];
  for (const sym of [...new Set(symbols)]) {
    if (existingSyms.has(sym)) {
      dupes.push(sym);
    } else {
      newSymbols.push(sym);
      existingSyms.add(sym); // prevent within-batch dupes
    }
  }

  if (newSymbols.length === 0) {
    errEl.textContent = `${dupes.join(", ")} already in the portfolio`;
    return;
  }

  // Add chips in validating state
  const newChips = newSymbols.map(sym => ({ symbol: sym, status: "validating" }));
  portfolioChips.push(...newChips);
  renderPortfolioChips();

  btn.disabled = true;
  btn.textContent = "Validating…";

  // Validate in parallel
  await Promise.allSettled(
    newChips.map(chip =>
      validateSymbol(chip.symbol)
        .then(() => { chip.status = "valid"; })
        .catch(() => { chip.status = "invalid"; })
    )
  );

  renderPortfolioChips();
  btn.disabled = false;
  btn.textContent = "Add";

  const invalidNew = newChips.filter(c => c.status === "invalid");
  let msg = "";
  if (dupes.length > 0) msg += `${dupes.join(", ")} already in portfolio. `;
  if (invalidNew.length > 0) msg += `${invalidNew.map(c => c.symbol).join(", ")} not found.`;
  if (msg) errEl.textContent = msg.trim();
}

async function handleSavePortfolio() {
  const nameInput = document.getElementById("portfolioNameInput");
  const errEl = document.getElementById("portfolioError");
  const btn = document.getElementById("portfolioSubmit");

  const name = nameInput.value.trim();
  if (!name) { errEl.textContent = "Please enter a portfolio name"; return; }

  // Gather valid chips (ignore invalid ones)
  const validSymbols = portfolioChips.filter(c => c.status === "valid").map(c => c.symbol);
  const invalidCount = portfolioChips.filter(c => c.status === "invalid").length;

  if (validSymbols.length === 0) {
    errEl.textContent = "Add at least one valid stock symbol.";
    return;
  }

  errEl.textContent = "";
  btn.disabled = true;
  btn.textContent = "Saving…";

  if (editingPortfolioId !== null) {
    // ── Edit mode ──
    const pf = state.portfolios.find(p => p.id === editingPortfolioId);
    if (!pf) { btn.disabled = false; btn.textContent = "Save Changes"; return; }

    const oldSymbols = new Set(pf.symbols);
    pf.name = name;
    pf.color = selectedPortfolioColor;
    pf.symbols = validSymbols;

    // Clean up removed stocks from visibleStocks
    for (const s of oldSymbols) {
      if (!validSymbols.includes(s)) state.visibleStocks.delete(s);
    }

    // Fetch stock RRG for any new symbols
    const newStocks = validSymbols.filter(s => !state.stockData[s]);
    if (newStocks.length > 0) {
      try {
        const result = await fetchStockRRG(newStocks);
        for (const [sym, data] of Object.entries(result.stocks)) {
          state.stockData[sym] = data;
        }
      } catch (err) {
        console.error("Failed to fetch new stock data:", err);
      }
    }

    // Re-fetch composite RRG
    try {
      const pfRRG = await fetchPortfolioRRG(validSymbols);
      state.portfolioRRG[pf.id] = pfRRG.portfolio;
    } catch (err) {
      console.error("Failed to update portfolio composite RRG:", err);
    }

    btn.disabled = false;
    btn.textContent = "Save Changes";
  } else {
    // ── Create mode ──
    const pf = {
      id: state._nextPortfolioId++,
      name: name,
      color: selectedPortfolioColor,
      symbols: validSymbols,
    };
    state.portfolios.push(pf);

    // Pre-fetch stock data
    try {
      const result = await fetchStockRRG(validSymbols);
      for (const [sym, data] of Object.entries(result.stocks)) {
        state.stockData[sym] = data;
      }
    } catch (err) {
      console.error("Failed to pre-fetch portfolio stock data:", err);
    }

    // Fetch composite RRG
    try {
      const pfRRG = await fetchPortfolioRRG(validSymbols);
      state.portfolioRRG[pf.id] = pfRRG.portfolio;
    } catch (err) {
      console.error("Failed to fetch portfolio composite RRG:", err);
    }

    btn.disabled = false;
    btn.textContent = "Create Portfolio";
  }

  if (invalidCount > 0) {
    // Don't close yet, show warning
    errEl.textContent = `${invalidCount} invalid symbol${invalidCount > 1 ? "s" : ""} were excluded.`;
  }

  closeAddSymbolModal();
  renderChart();
  renderSidebar();
}

/* Open the modal in edit mode for a specific portfolio */
function openEditPortfolioModal(portfolioId) {
  const pf = state.portfolios.find(p => p.id === portfolioId);
  if (!pf) return;

  editingPortfolioId = portfolioId;

  const modal = document.getElementById("addSymbolModal");
  modal.classList.add("visible");

  // Switch to portfolio tab
  switchModalTab("portfolio");

  // Pre-fill name
  document.getElementById("portfolioNameInput").value = pf.name;

  // Pre-fill stocks as valid chips
  portfolioChips = pf.symbols.map(sym => ({ symbol: sym, status: "valid" }));
  renderPortfolioChips();

  // Pre-select color
  selectedPortfolioColor = pf.color;
  initColorPicker();

  // Clear input & error
  document.getElementById("portfolioStocksInput").value = "";
  document.getElementById("portfolioError").textContent = "";

  // Update submit button text
  document.getElementById("portfolioSubmit").textContent = "Save Changes";

  // Focus the stocks input so user can start adding
  document.getElementById("portfolioStocksInput").focus();
}

/* ── Resize ── */
let resizeTimer;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    if (Object.keys(state.sectors).length > 0) renderChart();
  }, 150);
});

/* ── Init ── */
document.addEventListener("DOMContentLoaded", async () => {
  renderBenchmarkButtons();
  renderTailButtons();

  // Load holdings metadata first
  try {
    state.holdings = await fetchHoldings();
  } catch (err) {
    console.error("Failed to load holdings:", err);
  }

  // Add symbol button
  document.getElementById("btnAddSymbol").addEventListener("click", openAddSymbolModal);

  // Toggle all button
  document.getElementById("btnToggleAll").addEventListener("click", () => {
    const allSymbols = Object.keys(state.sectors);
    const pfKeys = state.portfolios.map(pf => `_pf_${pf.id}`);
    const allItems = [...allSymbols, ...pfKeys];
    const allHidden = allItems.length > 0 && allItems.every(s => state.dimmedSymbols.has(s));
    if (allHidden) {
      state.dimmedSymbols.clear();
    } else {
      allItems.forEach(s => state.dimmedSymbols.add(s));
    }
    renderChart();
    renderSidebar();
  });

  // Modal close
  document.getElementById("addSymbolClose").addEventListener("click", closeAddSymbolModal);
  document.getElementById("addSymbolModal").addEventListener("click", (e) => {
    if (e.target.id === "addSymbolModal") closeAddSymbolModal();
  });

  // Modal tabs
  document.querySelectorAll(".modal-tab").forEach(tab => {
    tab.addEventListener("click", () => switchModalTab(tab.dataset.tab));
  });

  // Add symbols submit
  document.getElementById("addSymbolSubmit").addEventListener("click", handleAddSymbol);
  document.getElementById("addSymbolInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") handleAddSymbol();
    if (e.key === "Escape") closeAddSymbolModal();
  });

  // Portfolio: add stocks button
  document.getElementById("portfolioAddStocksBtn").addEventListener("click", handleAddPortfolioStocks);
  document.getElementById("portfolioStocksInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") handleAddPortfolioStocks();
    if (e.key === "Escape") closeAddSymbolModal();
  });

  // Portfolio: save/create submit
  document.getElementById("portfolioSubmit").addEventListener("click", handleSavePortfolio);

  loadData();
});
