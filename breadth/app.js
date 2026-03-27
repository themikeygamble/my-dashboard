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
  if (!entry) return { sector: "", industry: "" };
  return {
    sector: entry.sector || "",
    industry: entry.industry || ""
  };
}

let tvWidget = null;
let tvScriptLoaded = false;
let tvScriptLoading = false;
let tvScriptCallbacks = [];
let currentItems = [];
let activeIndex = -1;

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

  loadTVScript(() => {
    if (tvWidget) {
      tvWidget.setSymbol(item.symbol, "D", () => {});
    } else {
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
    }
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
  const grid = document.getElementById("tickerGrid");
  const input = document.getElementById("tickerSearch");
  const copyBtn = document.getElementById("copyBtn");

  title.textContent = label;
  meta.textContent = date;
  input.value = "";

  const baseItems = normalizeListItems(rawItems);

  const refresh = (itemsToRender) => {
    currentItems = sortLeaderboardItems(itemsToRender);
    count.textContent = `${currentItems.length.toLocaleString()} symbols`;
    renderLeaderboard(grid, currentItems);
    selectTicker(0);
  };

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

document.getElementById("closeModal").addEventListener("click", () => {
  document.getElementById("modal").classList.add("hidden");
});

document.getElementById("modal").addEventListener("click", (e) => {
  if (e.target.id === "modal") {
    document.getElementById("modal").classList.add("hidden");
  }
});

document.getElementById("prevBtn").addEventListener("click", () => selectTicker(activeIndex - 1));
document.getElementById("nextBtn").addEventListener("click", () => selectTicker(activeIndex + 1));

document.addEventListener("keydown", (e) => {
  if (document.getElementById("modal").classList.contains("hidden")) return;

  if (e.key === "Escape") {
    document.getElementById("modal").classList.add("hidden");
    return;
  }

  if (e.key === "ArrowDown" || e.key === "ArrowRight") {
    e.preventDefault();
    selectTicker(activeIndex + 1);
  } else if (e.key === "ArrowUp" || e.key === "ArrowLeft") {
    e.preventDefault();
    selectTicker(activeIndex - 1);
  }
});

loadData();
