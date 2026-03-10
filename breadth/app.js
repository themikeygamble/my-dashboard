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
let selectedYear = "2026";

async function loadData() {
  const res = await fetch("../data/breadth-history.json", { cache: "no-store" });
  const json = await res.json();
  breadthData = json.rows || [];

  if (!breadthData.length) {
    renderYearTabs([]);
    renderTable([]);
    document.getElementById("nasdaqPrice").textContent = "--";
    document.getElementById("lastUpdated").textContent = "--";
    document.getElementById("subhead").textContent = "Interactive market monitor";
    return;
  }

  renderYearTabs(breadthData);

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
    if (ratio >= 2) {
      return { upClass: "bull-dominant", downClass: "bull-mid" };
    }
    return { upClass: "bull-dominant", downClass: "bull-soft" };
  }

  if (ratio < 1) {
    if (ratio <= 0.5) {
      return { upClass: "bear-soft", downClass: "bear-dominant" };
    }
    return { upClass: "bear-mid", downClass: "bear-dominant" };
  }

  return { upClass: "badge-neutral", downClass: "badge-neutral" };
}

function getRatioClass(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "plain-num";
  }

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
      return {
        symbol: item,
        percent: null
      };
    }

    const rawPercent =
      item.percent ??
      item.pct ??
      item.pct_change ??
      item.pctChange ??
      item.changePercent ??
      item.percentage ??
      item.value;

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

function getMagnitude(value) {
  return Number.isFinite(value) ? Math.abs(value) : -Infinity;
}

function sortLeaderboardItems(items) {
  return [...items].sort((a, b) => {
    const magDiff = getMagnitude(b.percent) - getMagnitude(a.percent);
    if (magDiff !== 0) return magDiff;

    const aPct = Number.isFinite(a.percent) ? a.percent : -Infinity;
    const bPct = Number.isFinite(b.percent) ? b.percent : -Infinity;
    return bPct - aPct;
  });
}

function renderLeaderboard(grid, items) {
  grid.innerHTML = "";

  const head = document.createElement("div");
  head.className = "leaderboard-head";
  head.innerHTML = `
    <span class="leader-rank">#</span>
    <span class="leader-symbol">Ticker</span>
    <span class="leader-percent">% Change</span>
  `;
  grid.appendChild(head);

  items.forEach((item, index) => {
    const row = document.createElement("div");
    row.className = "leaderboard-row";
    row.innerHTML = `
      <span class="leader-rank">${index + 1}</span>
      <span class="leader-symbol">${item.symbol}</span>
      <span class="leader-percent">${formatPercent(item.percent)}</span>
    `;
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
    const sorted = sortLeaderboardItems(itemsToRender);
    count.textContent = `${sorted.length.toLocaleString()} symbols`;
    renderLeaderboard(grid, sorted);
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
    setTimeout(() => {
      copyBtn.textContent = "Copy list";
    }, 1200);
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
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  if (!Number.isFinite(Number(value))) {
    return "∞";
  }

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

loadData();
