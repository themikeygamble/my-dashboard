(function () {
  const METRIC_GROUPS = {
    incomeCharts: [
      { key: "revenue", title: "Revenue", unit: "currency", subtitle: "Annual top-line sales" },
      { key: "gross_profit", title: "Gross Profit", unit: "currency", subtitle: "Revenue minus cost of goods sold" },
      { key: "operating_income", title: "Operating Income", unit: "currency", subtitle: "Core operating profitability" },
      { key: "net_income", title: "Net Income", unit: "currency", subtitle: "Bottom-line earnings" },
      { key: "operating_cash_flow", title: "Cash Flow", unit: "currency", subtitle: "Cash from operations" },
      { key: "operating_expense", title: "OpEx", unit: "currency", subtitle: "Operating expenses" },
      { key: "eps", title: "Earnings Per Share", unit: "ratio", subtitle: "Diluted or best available EPS" }
    ],
    balanceCharts: [
      { key: "total_assets", title: "Total Assets", unit: "currency", subtitle: "Balance sheet asset base" },
      { key: "total_liabilities", title: "Total Liabilities", unit: "currency", subtitle: "Balance sheet obligations" },
      { key: "cash", title: "Cash", unit: "currency", subtitle: "Cash and equivalents" },
      { key: "working_capital", title: "Working Capital", unit: "currency", subtitle: "Current assets minus current liabilities" },
      { key: "share_count", title: "Share Count", unit: "shares", subtitle: "Weighted average or outstanding shares" }
    ],
    marginCharts: [
      { key: "gross_margin", title: "Gross Margin", unit: "percent", subtitle: "Gross profit as a percent of revenue" },
      { key: "operating_margin", title: "Operating Margin", unit: "percent", subtitle: "Operating income as a percent of revenue" },
      { key: "net_margin", title: "Net Margin", unit: "percent", subtitle: "Net income as a percent of revenue" }
    ]
  };

  const BREADTH_LABELS = {
    up4_today: "Up 4%+",
    down4_today: "Down 4%+",
    up25_quarter: "Up 25%+ Qtr",
    down25_quarter: "Down 25%+ Qtr",
    up25_month: "Up 25%+ Month",
    down25_month: "Down 25%+ Month",
    up50_month: "Up 50%+ Month",
    down50_month: "Down 50%+ Month",
    up13_34d: "Up 13%+ / 34D",
    down13_34d: "Down 13%+ / 34D"
  };

  let fundamentalsPayload = null;
  let breadthPayload = null;
  let sectorMap = {};
  let activeSymbol = "";
  let activePane = "chart";

  window.BREADTH_MODAL_FUNDAMENTALS = {
    showSymbol(symbol) {
      activeSymbol = symbol || "";
      if (activePane === "fundamentals") {
        renderSymbol();
      }
    },
    close() {
      activeSymbol = "";
      activePane = "chart";
      syncPane();
    }
  };

  init();

  async function init() {
    bindTabs();

    const [fundamentalsRes, breadthRes, sectorRes] = await Promise.allSettled([
      fetch("../data/fundamentals.json", { cache: "no-store" }),
      fetch("../data/breadth-history.json", { cache: "no-store" }),
      fetch("../data/sector-map.json", { cache: "no-store" })
    ]);

    if (fundamentalsRes.status === "fulfilled" && fundamentalsRes.value.ok) {
      fundamentalsPayload = await fundamentalsRes.value.json();
    }
    if (breadthRes.status === "fulfilled" && breadthRes.value.ok) {
      breadthPayload = await breadthRes.value.json();
    }
    if (sectorRes.status === "fulfilled" && sectorRes.value.ok) {
      sectorMap = await sectorRes.value.json();
    }
  }

  function bindTabs() {
    document.getElementById("chartPaneBtn").addEventListener("click", () => {
      activePane = "chart";
      syncPane();
    });

    document.getElementById("fundamentalsPaneBtn").addEventListener("click", () => {
      activePane = "fundamentals";
      syncPane();
      renderSymbol();
    });
  }

  function syncPane() {
    document.getElementById("chartPaneBtn").classList.toggle("active", activePane === "chart");
    document.getElementById("fundamentalsPaneBtn").classList.toggle("active", activePane === "fundamentals");
    document.getElementById("chartPane").classList.toggle("hidden", activePane !== "chart");
    document.getElementById("fundamentalsPane").classList.toggle("hidden", activePane !== "fundamentals");
  }

  function renderSymbol() {
    if (!activeSymbol || !fundamentalsPayload?.symbols?.[activeSymbol]) {
      showEmpty("No SEC fundamentals cached for this ticker yet.");
      return;
    }

    const entry = fundamentalsPayload.symbols[activeSymbol];
    const sectorInfo = sectorMap[activeSymbol] || {};
    const companyMeta = [entry.exchange, sectorInfo.sector || entry.sector, sectorInfo.industry || entry.industry]
      .filter(Boolean)
      .join(" | ");

    document.getElementById("fundCompanyName").textContent = entry.company_name || activeSymbol;
    document.getElementById("fundCompanyMeta").textContent = companyMeta || "SEC annual filing history";
    document.getElementById("fundLatestFiscalYear").textContent = entry.latest_fiscal_year || "--";
    document.getElementById("fundLastFiled").textContent = entry.last_filed || "--";

    const contexts = buildBreadthContext(activeSymbol);
    document.getElementById("fundBreadthHits").textContent = String(contexts.length);
    renderBreadthContext(contexts);

    Object.entries(METRIC_GROUPS).forEach(([containerId, metrics]) => {
      renderMetricGroup(containerId, entry, metrics);
    });

    document.getElementById("fundamentalsEmptyState").classList.add("hidden");
    document.getElementById("fundamentalsContent").classList.remove("hidden");
  }

  function showEmpty(message) {
    document.getElementById("fundamentalsEmptyState").textContent = message;
    document.getElementById("fundamentalsEmptyState").classList.remove("hidden");
    document.getElementById("fundamentalsContent").classList.add("hidden");
  }

  function buildBreadthContext(symbol) {
    const rows = breadthPayload?.rows || [];
    const contexts = [];
    for (const row of rows.slice(0, 30)) {
      for (const [columnKey, label] of Object.entries(BREADTH_LABELS)) {
        const found = (row.lists?.[columnKey] || []).find(item => normalizeSymbol(item) === symbol);
        if (!found) continue;
        contexts.push({
          date: row.date,
          label,
          percent: extractPercent(found)
        });
        if (contexts.length >= 8) return contexts;
      }
    }
    return contexts;
  }

  function renderBreadthContext(contexts) {
    const container = document.getElementById("fundBreadthContext");
    container.innerHTML = "";
    if (!contexts.length) {
      container.innerHTML = `<div class="chart-empty">No recent breadth hits for this ticker.</div>`;
      return;
    }

    contexts.forEach(context => {
      const card = document.createElement("div");
      card.className = "context-chip";
      card.innerHTML = `<strong>${context.label}</strong><span>${context.date}${context.percent === null ? "" : ` | ${formatPercent(context.percent)}`}</span>`;
      container.appendChild(card);
    });
  }

  function renderMetricGroup(containerId, entry, metrics) {
    const container = document.getElementById(containerId);
    container.innerHTML = "";

    metrics.forEach(metric => {
      const card = document.createElement("article");
      card.className = "chart-card";
      card.innerHTML = `<h4>${metric.title}</h4><p class="chart-subtitle">${metric.subtitle}</p>`;

      const series = entry.metrics?.[metric.key] || [];
      if (!series.length) {
        const empty = document.createElement("div");
        empty.className = "chart-empty";
        empty.textContent = "No annual SEC data found for this metric.";
        card.appendChild(empty);
      } else {
        card.appendChild(buildBarChart(series, metric.unit));
        const footer = document.createElement("div");
        footer.className = "chart-footer";
        footer.innerHTML = `<span>${series.length} annual period${series.length === 1 ? "" : "s"}</span><span>${series[series.length - 1].filed || series[series.length - 1].end || ""}</span>`;
        card.appendChild(footer);
      }

      container.appendChild(card);
    });
  }

  function buildBarChart(series, unit) {
    const chart = document.createElement("div");
    chart.className = "bar-chart";
    const values = series.map(point => Number(point.value)).filter(Number.isFinite);
    const hasNegative = values.some(value => value < 0);
    const maxAbs = Math.max(...values.map(value => Math.abs(value)), 1);

    series.forEach(point => {
      const value = Number(point.value);
      const slot = document.createElement("div");
      slot.className = "bar-slot";

      const valueEl = document.createElement("div");
      valueEl.className = "bar-value";
      valueEl.textContent = formatMetricValue(value, unit);
      slot.appendChild(valueEl);

      const track = document.createElement("div");
      track.className = `bar-track ${hasNegative ? "negative-track" : "positive-track"}`;

      const fill = document.createElement("div");
      fill.className = `bar-fill ${value < 0 ? "negative" : unit === "percent" ? "positive" : ""}`;
      fill.style.height = `${Math.max((Math.abs(value) / maxAbs) * (hasNegative ? 50 : 100), 4)}%`;
      if (!hasNegative || value >= 0) {
        fill.style.bottom = hasNegative ? "50%" : "0";
        fill.style.top = "auto";
      }

      track.appendChild(fill);
      slot.appendChild(track);

      const label = document.createElement("div");
      label.className = "bar-label";
      label.textContent = point.label || point.fy || point.end || "--";
      slot.appendChild(label);

      chart.appendChild(slot);
    });

    return chart;
  }

  function normalizeSymbol(item) {
    if (typeof item === "string") return item.toUpperCase();
    return String(item?.symbol || item?.ticker || "").toUpperCase();
  }

  function extractPercent(item) {
    if (typeof item === "string") return null;
    const value = item?.percent ?? item?.pct ?? item?.pctChange ?? item?.changePercent;
    return Number.isFinite(Number(value)) ? Number(value) : null;
  }

  function formatMetricValue(value, unit) {
    if (!Number.isFinite(value)) return "--";
    if (unit === "percent") return `${value.toFixed(1)}%`;
    if (unit === "ratio") return value.toFixed(2);
    if (unit === "shares") return abbreviateNumber(value);
    return abbreviateCurrency(value);
  }

  function abbreviateCurrency(value) {
    const abs = Math.abs(value);
    if (abs >= 1e12) return `${value < 0 ? "-" : ""}$${(abs / 1e12).toFixed(2)}T`;
    if (abs >= 1e9) return `${value < 0 ? "-" : ""}$${(abs / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${value < 0 ? "-" : ""}$${(abs / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `${value < 0 ? "-" : ""}$${(abs / 1e3).toFixed(1)}K`;
    return `${value < 0 ? "-" : ""}$${abs.toFixed(0)}`;
  }

  function abbreviateNumber(value) {
    const abs = Math.abs(value);
    if (abs >= 1e12) return `${value < 0 ? "-" : ""}${(abs / 1e12).toFixed(2)}T`;
    if (abs >= 1e9) return `${value < 0 ? "-" : ""}${(abs / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${value < 0 ? "-" : ""}${(abs / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `${value < 0 ? "-" : ""}${(abs / 1e3).toFixed(1)}K`;
    return value.toFixed(0);
  }

  function formatPercent(value) {
    if (!Number.isFinite(value)) return "--";
    return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
  }
})();
