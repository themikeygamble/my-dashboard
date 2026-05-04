const LIST_COLUMNS = [
  { key: "up4_today", label: "Up 4%+" },
  { key: "down4_today", label: "Down 4%+" },
  { key: "up25_quarter", label: "Up 25%+ Qtr" },
  { key: "down25_quarter", label: "Down 25%+ Qtr" },
  { key: "up25_month", label: "Up 25%+ Month" },
  { key: "down25_month", label: "Down 25%+ Month" },
  { key: "up50_month", label: "Up 50%+ Month" },
  { key: "down50_month", label: "Down 50%+ Month" },
  { key: "up13_34d", label: "Up 13%+ / 34D" },
  { key: "down13_34d", label: "Down 13%+ / 34D" }
];

const GROUPING = document.body.dataset.grouping === "industry" ? "industry" : "sector";
const GROUP_LABEL = GROUPING === "industry" ? "Industry" : "Sector";

const dateSelect = document.getElementById("dateSelect");
const groupTableHead = document.getElementById("groupTableHead");
const groupTableBody = document.getElementById("groupTableBody");
const selectedDateEl = document.getElementById("selectedDate");
const groupCountEl = document.getElementById("groupCount");
const pageTitle = document.getElementById("pageTitle");
const subhead = document.getElementById("subhead");

let breadthRows = [];
let sectorMap = {};
let nameMap = {};
let metricsLookup = new Map();
let selectedDate = "";

if (pageTitle) pageTitle.textContent = `${GROUP_LABEL} Breadth`;
if (subhead) subhead.textContent = `Market breadth by ${GROUP_LABEL.toLowerCase()}`;
document.title = `${GROUP_LABEL} Breadth`;

function formatDollarVolume(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`;
  if (value >= 1e6) return `$${(value / 1e6).toFixed(0)}M`;
  if (value >= 1e3) return `$${(value / 1e3).toFixed(0)}K`;
  return `$${value.toFixed(0)}`;
}

function formatAdr(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value.toFixed(2)}%`;
}

function normalizeNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractSymbol(item) {
  if (!item) return "";
  if (typeof item === "string") return item;
  return item.symbol || item.ticker || "";
}

function buildNameMap(entries) {
  const map = {};
  (entries || []).forEach(entry => {
    if (!entry) return;
    const symbol = extractSymbol(entry);
    if (!symbol) return;
    const name = typeof entry === "string"
      ? ""
      : (entry.name || entry.company_name || entry.companyName || entry.longName || entry.shortName || "");
    if (name) map[symbol] = name;
  });
  return map;
}

function buildMetricsLookup(rows) {
  const lookup = new Map();
  (rows || []).forEach(row => {
    const date = row?.date;
    const lists = row?.lists;
    if (!date || !lists) return;
    const listLookup = new Map();
    Object.entries(lists).forEach(([key, items]) => {
      if (!Array.isArray(items)) return;
      const symbolLookup = new Map();
      items.forEach(item => {
        if (!item) return;
        const symbol = extractSymbol(item);
        if (!symbol) return;
        symbolLookup.set(symbol, {
          dollar_volume: normalizeNumber(item.dollar_volume ?? item.dollarVolume ?? null),
          adr_pct: normalizeNumber(item.adr_pct ?? item.adrPct ?? null)
        });
      });
      if (symbolLookup.size) listLookup.set(key, symbolLookup);
    });
    if (listLookup.size) lookup.set(date, listLookup);
  });
  return lookup;
}

function getMetricsEntry(date, listKey, symbol) {
  if (!date || !listKey || !symbol) return null;
  return metricsLookup.get(date)?.get(listKey)?.get(symbol) ?? null;
}

function normalizeItem(item, date, listKey) {
  const rawSymbol = extractSymbol(item);
  if (typeof item === "string") {
    const metrics = rawSymbol ? (getMetricsEntry(date, listKey, rawSymbol) || {}) : {};
    return {
      symbol: rawSymbol,
      dollar_volume: metrics.dollar_volume ?? null,
      adr_pct: metrics.adr_pct ?? null
    };
  }
  const metrics = rawSymbol ? (getMetricsEntry(date, listKey, rawSymbol) || {}) : {};
  const dollarVolume = normalizeNumber(item.dollar_volume ?? item.dollarVolume ?? null);
  const adrPct = normalizeNumber(item.adr_pct ?? item.adrPct ?? null);
  return {
    symbol: rawSymbol,
    dollar_volume: dollarVolume ?? metrics.dollar_volume ?? null,
    adr_pct: adrPct ?? metrics.adr_pct ?? null
  };
}

function getGroupInfo(symbol) {
  const entry = sectorMap[symbol] || {};
  const groupValue = GROUPING === "industry" ? entry.industry : entry.sector;
  return {
    group: groupValue || "Unknown",
    name: entry.name || nameMap[symbol] || "",
    sector: entry.sector || "",
    industry: entry.industry || ""
  };
}

function renderHeader() {
  if (!groupTableHead) return;
  groupTableHead.innerHTML = "";
  const row = document.createElement("tr");
  const groupTh = document.createElement("th");
  groupTh.textContent = GROUP_LABEL;
  row.appendChild(groupTh);
  LIST_COLUMNS.forEach(col => {
    const th = document.createElement("th");
    th.textContent = col.label;
    row.appendChild(th);
  });
  groupTableHead.appendChild(row);
}

function buildGroupData(row) {
  const groups = new Map();

  LIST_COLUMNS.forEach(col => {
    const list = row.lists?.[col.key] || [];
    list.forEach(rawItem => {
      const item = normalizeItem(rawItem, row.date, col.key);
      if (!item.symbol) return;
      const meta = getGroupInfo(item.symbol);
      const groupKey = meta.group;

      if (!groups.has(groupKey)) {
        groups.set(groupKey, {
          name: groupKey,
          counts: {},
          items: new Map()
        });
      }

      const group = groups.get(groupKey);
      group.counts[col.key] = (group.counts[col.key] || 0) + 1;

      if (!group.items.has(item.symbol)) {
        group.items.set(item.symbol, {
          symbol: item.symbol,
          name: meta.name,
          sector: meta.sector,
          industry: meta.industry,
          dollar_volume: item.dollar_volume,
          adr_pct: item.adr_pct,
          categories: new Set([col.key])
        });
      } else {
        const existing = group.items.get(item.symbol);
        existing.categories.add(col.key);
        if (existing.dollar_volume === null && item.dollar_volume !== null) {
          existing.dollar_volume = item.dollar_volume;
        }
        if (existing.adr_pct === null && item.adr_pct !== null) {
          existing.adr_pct = item.adr_pct;
        }
      }
    });
  });

  return Array.from(groups.values()).map(group => ({
    ...group,
    total: group.items.size
  })).sort((a, b) => {
    if (b.total !== a.total) return b.total - a.total;
    return a.name.localeCompare(b.name);
  });
}

function formatCategoryList(categories) {
  return LIST_COLUMNS
    .filter(col => categories.has(col.key))
    .map(col => col.label)
    .join(", ");
}

function buildDetailTable(group) {
  const wrap = document.createElement("div");
  wrap.className = "detail-wrap";

  const items = Array.from(group.items.values()).sort((a, b) =>
    a.symbol.localeCompare(b.symbol)
  );

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "detail-empty";
    empty.textContent = "No stocks available.";
    wrap.appendChild(empty);
    return wrap;
  }

  const table = document.createElement("table");
  table.className = "detail-table";
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["Ticker", "Company", "$ Volume", "ADR%", "Categories"].forEach(label => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  items.forEach(item => {
    const row = document.createElement("tr");

    const tickerTd = document.createElement("td");
    tickerTd.textContent = item.symbol;
    row.appendChild(tickerTd);

    const nameTd = document.createElement("td");
    nameTd.textContent = item.name || "—";
    row.appendChild(nameTd);

    const volTd = document.createElement("td");
    volTd.className = "num";
    volTd.textContent = formatDollarVolume(item.dollar_volume);
    row.appendChild(volTd);

    const adrTd = document.createElement("td");
    adrTd.className = "num";
    adrTd.textContent = formatAdr(item.adr_pct);
    row.appendChild(adrTd);

    const catTd = document.createElement("td");
    catTd.className = "cat";
    catTd.textContent = formatCategoryList(item.categories) || "—";
    row.appendChild(catTd);

    tbody.appendChild(row);
  });
  table.appendChild(tbody);

  wrap.appendChild(table);
  return wrap;
}

function renderTable(groups) {
  if (!groupTableBody) return;
  groupTableBody.innerHTML = "";

  if (!groups.length) {
    const emptyRow = document.createElement("tr");
    const emptyCell = document.createElement("td");
    emptyCell.colSpan = LIST_COLUMNS.length + 1;
    emptyCell.className = "table-empty";
    emptyCell.textContent = "No breadth data available for this date.";
    emptyRow.appendChild(emptyCell);
    groupTableBody.appendChild(emptyRow);
    return;
  }

  groups.forEach(group => {
    const row = document.createElement("tr");
    row.className = "group-row";

    const nameCell = document.createElement("td");
    nameCell.className = "group-name-cell";

    const toggleBtn = document.createElement("button");
    toggleBtn.className = "toggle-btn";
    toggleBtn.type = "button";
    toggleBtn.setAttribute("aria-expanded", "false");
    toggleBtn.innerHTML = `<svg viewBox="0 0 12 12" fill="none"><path d="M4.5 2.5L8 6L4.5 9.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>`;

    const nameSpan = document.createElement("span");
    nameSpan.className = "group-name";
    nameSpan.textContent = group.name;

    const countSpan = document.createElement("span");
    countSpan.className = "group-count";
    countSpan.textContent = `${group.total.toLocaleString()} stock${group.total === 1 ? "" : "s"}`;

    nameCell.appendChild(toggleBtn);
    nameCell.appendChild(nameSpan);
    nameCell.appendChild(countSpan);
    row.appendChild(nameCell);

    LIST_COLUMNS.forEach(col => {
      const td = document.createElement("td");
      const count = group.counts[col.key] || 0;
      td.textContent = count.toLocaleString();
      row.appendChild(td);
    });

    const detailRow = document.createElement("tr");
    detailRow.className = "group-detail-row hidden";
    const detailCell = document.createElement("td");
    detailCell.colSpan = LIST_COLUMNS.length + 1;
    detailCell.appendChild(buildDetailTable(group));
    detailRow.appendChild(detailCell);

    toggleBtn.addEventListener("click", () => {
      const isHidden = detailRow.classList.toggle("hidden");
      toggleBtn.classList.toggle("expanded", !isHidden);
      toggleBtn.setAttribute("aria-expanded", String(!isHidden));
    });

    groupTableBody.appendChild(row);
    groupTableBody.appendChild(detailRow);
  });
}

function updateTopbar(groups) {
  if (selectedDateEl) selectedDateEl.textContent = selectedDate || "--";
  if (groupCountEl) groupCountEl.textContent = groups.length.toLocaleString();
}

function renderSelectedDate() {
  const row = breadthRows.find(r => r.date === selectedDate);
  const groups = row ? buildGroupData(row) : [];
  renderTable(groups);
  updateTopbar(groups);
}

function populateDates() {
  if (!dateSelect) return;
  dateSelect.innerHTML = "";
  const dates = [...new Set(breadthRows.map(r => r.date))]
    .sort((a, b) => (a > b ? -1 : 1));

  if (!dates.length) return;

  selectedDate = dates[0];
  dates.forEach(date => {
    const opt = document.createElement("option");
    opt.value = date;
    opt.textContent = date;
    if (date === selectedDate) opt.selected = true;
    dateSelect.appendChild(opt);
  });

  dateSelect.addEventListener("change", () => {
    selectedDate = dateSelect.value;
    renderSelectedDate();
  });
}

async function loadData() {
  const [breadthRes, sectorRes, metricsRes] = await Promise.allSettled([
    fetch("../../data/breadth-history.json", { cache: "no-store" }),
    fetch("../../data/sector-map.json", { cache: "no-store" }),
    fetch("../../data/breadth-metrics.json", { cache: "no-store" })
  ]);

  if (breadthRes.status === "fulfilled" && breadthRes.value.ok) {
    const json = await breadthRes.value.json();
    breadthRows = json.rows || [];
    nameMap = buildNameMap(json.universe?.symbols || []);
  }

  if (sectorRes.status === "fulfilled" && sectorRes.value.ok) {
    sectorMap = await sectorRes.value.json();
  }

  if (metricsRes.status === "fulfilled" && metricsRes.value.ok) {
    const metricsJson = await metricsRes.value.json();
    metricsLookup = buildMetricsLookup(metricsJson.rows || []);
  }

  renderHeader();
  populateDates();

  if (!breadthRows.length) {
    selectedDate = "";
    renderTable([]);
    updateTopbar([]);
    return;
  }

  renderSelectedDate();
}

loadData();
