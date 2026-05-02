const COLUMNS = [
  { key: "nasdaq_close", label: "NASDAQ", type: "value" },
  { key: "up4_today", label: "Up 4%+", type: "list" },
  { key: "down4_today", label: "Down 4%+", type: "list" },
  { key: "ratio_5d", label: "5D Ratio", type: "value" },
  { key: "ratio_10d", label: "10D Ratio", type: "value" },
  { key: "up25_quarter", label: "Up 25%+ Qtr", type: "list" },
  { key: "down25_quarter", label: "Down 25%+ Qtr", type: "list" },
  { key: "up25_month", label: "Up 25%+ Month", type: "list" },
  { key: "down25_month", label: "Down 25%+ Month", type: "list" },
  { key: "up50_month", label: "Up 50%+ Month", type: "list" },
  { key: "down50_month", label: "Down 50%+ Month", type: "list" },
  { key: "up13_34d", label: "Up 13%+ / 34D", type: "list" },
  { key: "down13_34d", label: "Down 13%+ / 34D", type: "list" }
];

const PAIRS = [
  ["up4_today", "down4_today"],
  ["up25_quarter", "down25_quarter"],
  ["up25_month", "down25_month"],
  ["up50_month", "down50_month"],
  ["up13_34d", "down13_34d"]
];

let breadthData = [];
let sectorMap = {};
let selectedYear = "2026";

async function loadData() {
  const [breadthRes, sectorRes] = await Promise.allSettled([
    fetch("../data/breadth-history.json", { cache: "no-store" }),
    fetch("../data/sector-map.json", { cache: "no-store" })
  ]);

  if (breadthRes.status === "fulfilled" && breadthRes.value.ok) {
    const json = await breadthRes.value.json();
    breadthData = json.rows || [];
  }

  if (sectorRes.status === "fulfilled" && sectorRes.value.ok) {
    sectorMap = await sectorRes.value.json();
  }

  if (!breadthData.length) {
    renderYearTabs([]);
    renderTable([]);
    document.getElementById("nasdaqPrice").textContent = "--";
    document.getElementById("lastUpdated").textContent = "--";
    document.getElementById("subhead").textContent = "Interactive market monitor";
    return;
  }

  const availableYears = getAvailableYears(breadthData);
  if (!availableYears.includes(selectedYear)) {
    selectedYear = availableYears[0];
  }

  renderYearTabs(breadthData);
  renderSelectedYear();
}

function getAvailableYears(rows) {
  return [...new Set(rows.map(r => String(r.date).slice(0, 4)))]
    .sort((a, b) => Number(b) - Number(a));
}

function renderYearTabs(rows) {
  const years = getAvailableYears(rows);
  const wrap = document.getElementById("yearTabs");
  wrap.innerHTML = "";

  years.forEach(year => {
    const btn = document.createElement("button");
    btn.className = `year-btn ${year === selectedYear ? "active" : ""}`;
    btn.textContent = year;
    btn.type = "button";
    btn.addEventListener("click", () => {
      selectedYear = year;
      renderYearTabs(breadthData);
      renderSelectedYear();
    });
    wrap.appendChild(btn);
  });
}

function renderSelectedYear() {
  const filtered = breadthData.filter(row => String(row.date).startsWith(selectedYear));

  if (!filtered.length) {
    document.getElementById("nasdaqPrice").textContent = "--";
    document.getElementById("lastUpdated").textContent = "--";
    document.getElementById("subhead").textContent = `Interactive market monitor • ${selectedYear}`;
    renderTable([]);
    return;
  }

  const latest = filtered[0];
  document.getElementById("nasdaqPrice").textContent = formatNumber(latest.nasdaq_close);
  document.getElementById("lastUpdated").textContent = latest.date;
  document.getElementById("subhead").textContent = `Interactive market monitor • ${selectedYear}`;

  renderTable(filtered);
}

function renderTable(rows) {
  const tbody = document.querySelector("#breadthTable tbody");
  tbody.innerHTML = "";

  rows.forEach(row => {
    const tr = document.createElement("tr");
    const pairClasses = buildPairClassMap(row);

    const dateTd = document.createElement("td");
    dateTd.textContent = row.date;
    tr.appendChild(dateTd);

    COLUMNS.forEach(col => {
      const td = document.createElement("td");

      if (col.type === "value") {
        const div = document.createElement("div");

        if (col.key === "ratio_5d" || col.key === "ratio_10d") {
          div.className = `metric-pill ${getRatioClass(row[col.key])}`;
          div.textContent = formatRatio(row[col.key]);
        } else {
          div.className = "metric-pill plain-num";
          div.textContent = formatNumber(row[col.key]);
        }

        td.appendChild(div);
      } else {
        const list = row.lists?.[col.key] || [];
        const btn = document.createElement("button");
        btn.className = `cell-btn ${pairClasses[col.key] || "plain-btn"}`;
        btn.textContent = list.length.toLocaleString();
        btn.addEventListener("click", () => openModal(row.date, col.label, list));
        td.appendChild(btn);
      }

      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });
}

function buildPairClassMap(row) {
  const out = {};

  for (const [upKey, downKey] of PAIRS) {
    const upCount = (row.lists?.[upKey] || []).length;
    const downCount = (row.lists?.[downKey] || []).length;
    const styles = getPairStyles(upCount, downCount);

    out[upKey] = styles.upClass;
    out[downKey] = styles.downClass;
  }

  return out;
}

function getPairStyles(upCount, downCount) {
  if (upCount === 0 && downCount === 0) {
    return { upClass: "plain-btn", downClass: "plain-btn" };
  }

  if (upCount === downCount) {
    return { upClass: "badge-neutral", downClass: "badge-neutral" };
  }

  if (downCount === 0 && upCount > 0) {
    return { upClass: "bull-dominant", downClass: "bull-soft" };
  }

  if (upCount === 0 && downCount > 0) {
    return { upClass: "bear-soft", downClass: "bear-dominant" };
  }

  const ratio = upCount / downCount;

  if (ratio > 1) {
    if (ratio >= 2) return { upClass: "bull-dominant", downClass: "bull-mid" };
    return { upClass: "bull-dominant", downClass: "bull-soft" };
  }

  if (ratio < 1) {
    if (ratio <= 0.5) return { upClass: "bear-soft", downClass: "bear-dominant" };
    return { upClass: "bear-mid", downClass: "bear-dominant" };
  }

  return { upClass: "badge-neutral", downClass: "badge-neutral" };
}

function getRatioClass(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "plain-num";
  const ratio = Number(value);
  if (ratio >= 2) return "bull-dominant";
  if (ratio > 1) return "bull-mid";
  if (ratio === 1) return "badge-neutral";
  if (ratio >= 0.5) return "bear-mid";
  return "bear-dominant";
}

function normalizeListItems(items) {
  return (items || []).map(item => {
    if (typeof item === "string") {
      return { symbol: item, percent: null };
    }

    const rawPercent =
      item.percent ?? item.pct ?? item.pct_change ??
      item.pctChange ?? item.changePercent ?? item.percentage ?? item.value;

    let percent = null;
    if (typeof rawPercent === "number" && Number.isFinite(rawPercent)) {
      percent = rawPercent;
    } else if (typeof rawPercent === "string") {
      const cleaned = rawPercent.replace("%", "").replace("+", "").trim();
      const parsed = Number(cleaned);
      percent = Number.isFinite(parsed) ? parsed : null;
    }

    return {
      symbol: item.symbol || item.ticker || item.name || "N/A",
      percent
    };
  });
}

function formatPercent(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function sortLeaderboardItems(items) {
  return [...items].sort((a, b) => {
    const aMag = Number.isFinite(a.percent) ? Math.abs(a.percent) : -Infinity;
    const bMag = Number.isFinite(b.percent) ? Math.abs(b.percent) : -Infinity;
    if (bMag !== aMag) return bMag - aMag;
    return (Number.isFinite(b.percent) ? b.percent : -Infinity) -
           (Number.isFinite(a.percent) ? a.percent : -Infinity);
  });
}

function getSectorInfo(symbol) {
  const entry = sectorMap[symbol];
  if (!entry) return { sector: "", industry: "", name: "" };
  return {
    sector: entry.sector || "",
    industry: entry.industry || "",
    name: entry.name || ""
  };
}

let tvWidget = null;
let tvScriptLoaded = false;
let tvScriptLoading = false;
let tvScriptCallbacks = [];
let currentItems = [];
let activeIndex = -1;
let modalMode = "list"; // "list" | "chart"

function loadTVScript(cb) {
  if (tvScriptLoaded) { cb(); return; }
  tvScriptCallbacks.push(cb);
  if (tvScriptLoading) return;
  tvScriptLoading = true;
  const s = document.createElement("script");
  s.src = "https://s3.tradingview.com/tv.js";
  s.onload = () => {
    tvScriptLoaded = true;
    tvScriptLoading = false;
    const cbs = tvScriptCallbacks.splice(0);
    cbs.forEach(fn => fn());
  };
  s.onerror = () => {
    tvScriptLoading = false;
    tvScriptCallbacks = [];
  };
  document.head.appendChild(s);
}

function formatDollarVolume(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`;
  if (value >= 1e6) return `$${(value / 1e6).toFixed(0)}M`;
  if (value >= 1e3) return `$${(value / 1e3).toFixed(0)}K`;
  return `$${value.toFixed(0)}`;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderStockList(items) {
  const tbody = document.getElementById("stockListBody");
  tbody.innerHTML = "";

  items.forEach((item, index) => {
    const { sector, industry, name } = getSectorInfo(item.symbol);
    const isPos = Number.isFinite(item.percent) && item.percent > 0;
    const isNeg = Number.isFinite(item.percent) && item.percent < 0;
    const chgClass = isPos ? " positive" : isNeg ? " negative" : "";
    const adrText = (item.adr_pct !== null && item.adr_pct !== undefined && Number.isFinite(item.adr_pct))
      ? item.adr_pct.toFixed(2) + "%" : "—";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="sl-rank-cell">${index + 1}</td>
      <td class="sl-ticker-cell">${escapeHtml(item.symbol)}</td>
      <td class="sl-name-cell" title="${escapeHtml(name || "—")}">${escapeHtml(name || "—")}</td>
      <td class="sl-muted-cell" title="${escapeHtml(sector || "—")}">${escapeHtml(sector || "—")}</td>
      <td class="sl-muted-cell" title="${escapeHtml(industry || "—")}">${escapeHtml(industry || "—")}</td>
      <td class="sl-num-cell">${formatDollarVolume(item.dollar_volume ?? null)}</td>
      <td class="sl-num-cell">${adrText}</td>
      <td class="sl-chg-cell${chgClass}">${formatPercent(item.percent)}</td>
      <td class="sl-actions-cell"><button class="chart-row-btn" type="button">Chart</button></td>
    `;

    tr.querySelector(".chart-row-btn").addEventListener("click", () => {
      switchToChartMode(index);
    });

    tbody.appendChild(tr);
  });
}

function switchToChartMode(index) {
  modalMode = "chart";
  document.getElementById("listPane").classList.add("hidden");
  document.getElementById("chartViewPane").classList.remove("hidden");
  document.getElementById("listViewBtn").classList.remove("active");
  document.getElementById("chartViewBtn").classList.add("active");

  renderLeaderboard(document.getElementById("tickerGrid"), currentItems);
  selectTicker(index);
}

function switchToListMode() {
  modalMode = "list";
  document.getElementById("chartViewPane").classList.add("hidden");
  document.getElementById("listPane").classList.remove("hidden");
  document.getElementById("chartViewBtn").classList.remove("active");
  document.getElementById("listViewBtn").classList.add("active");

  document.getElementById("tvChartContainer").innerHTML = "";
  tvWidget = null;
}

function selectTicker(index) {
  if (!currentItems.length) return;
  index = Math.max(0, Math.min(index, currentItems.length - 1));
  activeIndex = index;

  const rows = document.querySelectorAll("#tickerGrid .leaderboard-row");
  rows.forEach((r, i) => r.classList.toggle("active", i === index));
  if (rows[index]) rows[index].scrollIntoView({ block: "nearest" });

  const item = currentItems[index];
  const { sector, industry } = getSectorInfo(item.symbol);
  const metaText = sector && industry ? `${sector} · ${industry}` : sector || industry || "—";

  document.getElementById("tvSymbol").textContent = item.symbol;
  const pctEl = document.getElementById("tvPercent");
  pctEl.textContent = formatPercent(item.percent);
  const isPos = Number.isFinite(item.percent) && item.percent > 0;
  const isNeg = Number.isFinite(item.percent) && item.percent < 0;
  pctEl.className = "tv-percent" + (isPos ? " positive" : isNeg ? " negative" : "");
  document.getElementById("tvMeta").textContent = metaText;
  if (window.BREADTH_MODAL_FUNDAMENTALS) {
    window.BREADTH_MODAL_FUNDAMENTALS.showSymbol(item.symbol);
  }

  loadTVScript(() => {
    document.getElementById("tvChartContainer").innerHTML = "";
    tvWidget = new window.TradingView.widget({
      container_id: "tvChartContainer",
      symbol: item.symbol,
      interval: "D",
      theme: "dark",
      timezone: "America/New_York",
      autosize: true,
      locale: "en"
    });
  });
}

function renderLeaderboard(grid, items) {
  grid.innerHTML = "";

  const head = document.createElement("div");
  head.className = "leaderboard-head";
  head.innerHTML = `
    <span class="leader-rank">#</span>
    <span class="leader-symbol">Ticker</span>
    <span class="leader-meta">Sector · Industry</span>
    <span class="leader-percent">% Change</span>
  `;
  grid.appendChild(head);

  items.forEach((item, index) => {
    const { sector, industry } = getSectorInfo(item.symbol);
    const metaText = sector && industry
      ? `${sector} · ${industry}`
      : sector || industry || "—";

    const isPos = Number.isFinite(item.percent) && item.percent > 0;
    const isNeg = Number.isFinite(item.percent) && item.percent < 0;
    const pctClass = isPos ? " positive" : isNeg ? " negative" : "";

    const row = document.createElement("div");
    row.className = "leaderboard-row";
    row.innerHTML = `
      <span class="leader-rank">${index + 1}</span>
      <span class="leader-symbol">${item.symbol}</span>
      <span class="leader-meta">${metaText}</span>
      <span class="leader-percent${pctClass}">${formatPercent(item.percent)}</span>
    `;
    row.addEventListener("click", () => selectTicker(index));
    grid.appendChild(row);
  });
}

function openModal(date, label, rawItems) {
  const modal = document.getElementById("modal");
  const title = document.getElementById("modalTitle");
  const meta = document.getElementById("modalMeta");
  const count = document.getElementById("modalCount");
  const input = document.getElementById("tickerSearch");
  const copyBtn = document.getElementById("copyBtn");

  title.textContent = label;
  meta.textContent = date;
  input.value = "";

  const baseItems = normalizeListItems(rawItems);

  const refresh = (itemsToRender) => {
    currentItems = sortLeaderboardItems(itemsToRender);
    count.textContent = `${currentItems.length.toLocaleString()} symbols`;
    renderStockList(currentItems);
    if (modalMode === "chart") {
      renderLeaderboard(document.getElementById("tickerGrid"), currentItems);
    }
  };

  // Reset to list mode whenever a new modal is opened
  modalMode = "list";
  document.getElementById("listPane").classList.remove("hidden");
  document.getElementById("chartViewPane").classList.add("hidden");
  document.getElementById("listViewBtn").classList.add("active");
  document.getElementById("chartViewBtn").classList.remove("active");
  document.getElementById("tvChartContainer").innerHTML = "";
  tvWidget = null;

  refresh(baseItems);

  input.oninput = () => {
    const q = input.value.trim().toUpperCase();
    const filtered = !q
      ? baseItems
      : baseItems.filter(item => item.symbol.toUpperCase().includes(q));
    refresh(filtered);
  };

  copyBtn.onclick = async () => {
    await navigator.clipboard.writeText(baseItems.map(item => item.symbol).join(", "));
    copyBtn.textContent = "Copied";
    setTimeout(() => { copyBtn.textContent = "Copy list"; }, 1200);
  };

  modal.classList.remove("hidden");
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") return "--";
  if (typeof value === "number" && Math.abs(value) >= 1000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return typeof value === "number"
    ? value.toFixed(2).replace(/\.00$/, "")
    : value;
}

function formatRatio(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  if (!Number.isFinite(Number(value))) return "∞";
  return Number(value).toFixed(2);
}

function closeModal() {
  document.getElementById("modal").classList.add("hidden");
  document.getElementById("tvChartContainer").innerHTML = "";
  tvWidget = null;
  modalMode = "list";
  if (window.BREADTH_MODAL_FUNDAMENTALS) {
    window.BREADTH_MODAL_FUNDAMENTALS.close();
  }
}

document.getElementById("closeModal").addEventListener("click", closeModal);

document.getElementById("modal").addEventListener("click", (e) => {
  if (e.target.id === "modal") {
    closeModal();
  }
});

document.getElementById("listViewBtn").addEventListener("click", switchToListMode);
document.getElementById("chartViewBtn").addEventListener("click", () => {
  if (modalMode !== "chart") {
    switchToChartMode(0);
  }
});

document.getElementById("prevBtn").addEventListener("click", () => selectTicker(activeIndex - 1));
document.getElementById("nextBtn").addEventListener("click", () => selectTicker(activeIndex + 1));

document.getElementById("closeTrendModal").addEventListener("click", () => {
  document.getElementById("trendModal").classList.add("hidden");
});

document.getElementById("trendModal").addEventListener("click", (e) => {
  if (e.target.id === "trendModal") {
    document.getElementById("trendModal").classList.add("hidden");
  }
});

document.addEventListener("keydown", (e) => {
  const modalOpen = !document.getElementById("modal").classList.contains("hidden");
  const trendOpen = !document.getElementById("trendModal").classList.contains("hidden");

  if (!modalOpen && !trendOpen) return;

  if (e.key === "Escape") {
    if (modalOpen) {
      if (modalMode === "chart") { switchToListMode(); return; }
      closeModal();
      return;
    }
    if (trendOpen) { document.getElementById("trendModal").classList.add("hidden"); return; }
    return;
  }

  // Arrow navigation only in chart mode
  if (!modalOpen || modalMode !== "chart") return;

  if (e.key === "ArrowDown" || e.key === "ArrowRight") {
    e.preventDefault();
    selectTicker(activeIndex + 1);
  } else if (e.key === "ArrowUp" || e.key === "ArrowLeft") {
    e.preventDefault();
    selectTicker(activeIndex - 1);
  }
});

function initHeaderClicks() {
  const headerCols = COLUMNS.filter(c => c.key !== "nasdaq_close");
  const ths = document.querySelectorAll("#breadthTable thead tr:nth-child(2) th");
  ths.forEach((th, i) => {
    const col = headerCols[i];
    if (!col) return;
    th.classList.add("clickable-header");
    th.title = `View trend for ${col.label}`;
    th.addEventListener("click", () => openTrendModal(col));
  });
}

function openTrendModal(col) {
  const filtered = breadthData.filter(row => String(row.date).startsWith(selectedYear));
  const sortedAsc = [...filtered].sort((a, b) => (a.date > b.date ? 1 : -1));

  const dataPoints = sortedAsc.map(row => {
    let value;
    if (col.type === "list") {
      value = (row.lists?.[col.key] || []).length;
    } else {
      value = row[col.key];
    }
    const num = Number(value);
    return Number.isFinite(num) ? { date: row.date, value: num } : null;
  }).filter(Boolean);

  document.getElementById("trendTitle").textContent = col.label;
  document.getElementById("trendMeta").textContent =
    `${selectedYear} · ${dataPoints.length} data point${dataPoints.length !== 1 ? "s" : ""}`;

  renderTrendChart(dataPoints);
  document.getElementById("trendModal").classList.remove("hidden");
}

function calculateSMA(data, period) {
  return data.map((point, index, arr) => {
    if (index < period - 1) return null; 
    let sum = 0;
    for (let i = 0; i < period; i++) {
      sum += arr[index - i].value;
    }
    return sum / period;
  });
}

function renderTrendChart(dataPoints) {
  const container = document.getElementById("trendChartContainer");
  container.innerHTML = "";

  if (!dataPoints.length) {
    const msg = document.createElement("p");
    msg.style.cssText = "color:var(--muted);padding:24px;text-align:center;";
    msg.textContent = "No data available for this period.";
    container.appendChild(msg);
    return;
  }

  const W = 860, H = 360;
  const ml = 64, mr = 24, mt = 24, mb = 56;
  const pw = W - ml - mr;
  const ph = H - mt - mb;
  const n = dataPoints.length;

  const values = dataPoints.map(p => p.value);
  let minV = Math.min(...values);
  let maxV = Math.max(...values);
  if (minV === maxV) { minV -= 1; maxV += 1; }
  const range = maxV - minV;

  const xOf = i => ml + (n > 1 ? (i / (n - 1)) * pw : pw / 2);
  const yOf = v => mt + ph - ((v - minV) / range) * ph;

  const svgNS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("width", "100%");
  svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
  svg.style.display = "block";

  function el(tag, attrs) {
    const e = document.createElementNS(svgNS, tag);
    Object.entries(attrs).forEach(([k, v]) => e.setAttribute(k, v));
    return e;
  }

  // Horizontal grid lines + Y labels
  const NUM_Y = 5;
  for (let i = 0; i <= NUM_Y; i++) {
    const y = mt + (i / NUM_Y) * ph;
    const val = maxV - (i / NUM_Y) * range;
    svg.appendChild(el("line", {
      x1: ml, y1: y, x2: W - mr, y2: y,
      stroke: "#30363d", "stroke-width": "1", "stroke-dasharray": i === NUM_Y ? "none" : "4 4"
    }));
    const lbl = el("text", {
      x: ml - 8, y: y + 4,
      "text-anchor": "end", fill: "#8b949e", "font-size": "11", "font-family": "inherit"
    });
    lbl.textContent = val % 1 === 0 ? val.toFixed(0) : val.toFixed(2);
    svg.appendChild(lbl);
  }

  // Area fill under the line
  const areaD = [
    `M ${xOf(0)} ${yOf(dataPoints[0].value)}`,
    ...dataPoints.slice(1).map((p, i) => `L ${xOf(i + 1)} ${yOf(p.value)}`),
    `L ${xOf(n - 1)} ${mt + ph}`,
    `L ${xOf(0)} ${mt + ph}`,
    "Z"
  ].join(" ");
  svg.appendChild(el("path", {
    d: areaD, fill: "rgba(88,166,255,0.08)", stroke: "none"
  }));

  // Line path
  const lineD = dataPoints.map((p, i) => `${i === 0 ? "M" : "L"} ${xOf(i)} ${yOf(p.value)}`).join(" ");
  svg.appendChild(el("path", {
    d: lineD, fill: "none", stroke: "#58a6ff", "stroke-width": "2", "stroke-linejoin": "round", "stroke-linecap": "round"
  }));

  // --- Start Added Code: Moving Averages ---
  const sma10 = calculateSMA(dataPoints, 10);
  const sma20 = calculateSMA(dataPoints, 20);

  // Helper to safely build SVG paths
  const createPath = (smaArray) => {
    return smaArray.map((v, i) => {
      if (v === null) return null;
      // Start a new path (M) if it's the first valid point
      const prefix = (i === 0 || smaArray[i - 1] === null) ? 'M' : 'L';
      return `${prefix} ${xOf(i)} ${yOf(v)}`;
    }).filter(Boolean).join(' '); // .filter(Boolean) removes all empty spaces safely
  };

  const sma10Path = createPath(sma10);
  if (sma10Path) {
    svg.appendChild(el('path', { 
      d: sma10Path, 
      fill: 'none', 
      stroke: '#bf73ff', // Purple
      'stroke-width': '1.5', // Bumped up slightly so it's easier to see
      'stroke-linejoin': 'round', 
      'stroke-linecap': 'round' 
    }));
  }

  const sma20Path = createPath(sma20);
  if (sma20Path) {
    svg.appendChild(el('path', { 
      d: sma20Path, 
      fill: 'none', 
      stroke: '#f1e05a', // Yellow
      'stroke-width': '1.5', 
      'stroke-linejoin': 'round', 
      'stroke-linecap': 'round' 
    }));
  }
  // --- End Added Code ---

  // X axis labels (avoid overlap: at most 12 labels)
  const labelStep = Math.max(1, Math.ceil(n / 12));
  dataPoints.forEach((p, i) => {
    if (i % labelStep !== 0 && i !== n - 1) return;
    const x = xOf(i);
    const lbl = el("text", {
      x, y: H - mb + 18,
      "text-anchor": "middle", fill: "#8b949e", "font-size": "11", "font-family": "inherit"
    });
    lbl.textContent = p.date.slice(5); // MM-DD
    svg.appendChild(lbl);
    svg.appendChild(el("line", {
      x1: x, y1: mt + ph, x2: x, y2: mt + ph + 4,
      stroke: "#30363d", "stroke-width": "1"
    }));
  });

  // Tooltip overlay
  const tooltip = document.createElement("div");
  tooltip.className = "trend-tooltip";
  tooltip.style.display = "none";
  document.body.appendChild(tooltip);

  // Dots with hover
  dataPoints.forEach((p, i) => {
    const x = xOf(i);
    const y = yOf(p.value);

    const hitArea = el("circle", {
      cx: x, cy: y, r: "12",
      fill: "transparent", style: "cursor:pointer"
    });
    const dot = el("circle", {
      cx: x, cy: y, r: "4",
      fill: "#58a6ff", stroke: "#0f2745", "stroke-width": "2",
      style: "pointer-events:none"
    });

    hitArea.addEventListener("mouseenter", (evt) => {
      dot.setAttribute("r", "6");
      dot.setAttribute("fill", "#a5d6ff");
      const dispVal = p.value % 1 === 0 ? p.value.toFixed(0) : p.value.toFixed(2);
      tooltip.textContent = `${p.date}  ·  ${dispVal}`;
      tooltip.style.display = "block";
    });
    hitArea.addEventListener("mousemove", (evt) => {
      tooltip.style.left = (evt.clientX + 14) + "px";
      tooltip.style.top = (evt.clientY - 30) + "px";
    });
    hitArea.addEventListener("mouseleave", () => {
      dot.setAttribute("r", "4");
      dot.setAttribute("fill", "#58a6ff");
      tooltip.style.display = "none";
    });

    svg.appendChild(dot);
    svg.appendChild(hitArea);
  });

  container.appendChild(svg);

  // Remove tooltip when modal closes
  const closeFn = () => {
    tooltip.remove();
    document.getElementById("closeTrendModal").removeEventListener("click", closeFn);
    document.getElementById("trendModal").removeEventListener("click", closeFn);
  };
  document.getElementById("closeTrendModal").addEventListener("click", closeFn);
  document.getElementById("trendModal").addEventListener("click", closeFn);
}

loadData();
initHeaderClicks();
