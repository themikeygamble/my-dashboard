/* ═══════════════════════════════════════
   SwingScan — app.js
═══════════════════════════════════════ */

// ── CONFIG ── Update this after deploying your Render backend ─────────────────
const API_BASE = 'https://YOUR-APP-NAME.onrender.com';

const DEFAULT_UNIVERSE = [
  "NVDA","AMD","AVGO","SMCI","ARM","TSM","AEHR","MRVL",
  "MSTR","COIN","HOOD","MARA","RIOT","CLSK","CIFR","HUT",
  "META","TSLA","PLTR","SOFI","UPST","CRWD","NET","DDOG","AXON",
  "IONQ","RGTI","QUBT","QBTS","ARQQ",
  "ACHR","JOBY","RKLB","LUNR","BLNK",
  "SNOW","BILL","RBLX","SHOP","U","CELH",
  "BABA","JD","PDD",
  "GME","SOUN","BBAI","CLOV",
];

// ── STATE ─────────────────────────────────────────────────────────────────────
let screenerCache = [];
let currentSort   = 'score';

// ── INIT ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('scan-date').textContent =
    new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

  // Tabs
  document.querySelectorAll('.tab-btn').forEach(btn =>
    btn.addEventListener('click', () => switchTab(btn.dataset.tab))
  );

  // Screener
  document.getElementById('scan-btn').addEventListener('click',    () => runScreener(false));
  document.getElementById('refresh-btn').addEventListener('click', () => runScreener(true));
  document.getElementById('sort-select').addEventListener('change', e => {
    currentSort = e.target.value;
    if (screenerCache.length) renderTable(sortData(screenerCache, currentSort));
  });

  // Rater
  document.getElementById('analyze-btn').addEventListener('click', analyzeStock);
  document.getElementById('ticker-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') analyzeStock();
  });

  // Quick picks
  document.querySelectorAll('.qp-btn').forEach(btn =>
    btn.addEventListener('click', () => {
      document.getElementById('ticker-input').value = btn.dataset.ticker;
      analyzeStock();
    })
  );

  // Modal
  document.querySelector('.modal-close').addEventListener('click', closeModal);
  document.querySelector('.modal-backdrop').addEventListener('click', closeModal);
  document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });
});

// ── TABS ──────────────────────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.tab-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.tab === tab)
  );
  document.querySelectorAll('.tab-panel').forEach(p => {
    p.classList.toggle('active', p.id === `${tab}-tab`);
    p.classList.toggle('hidden',  p.id !== `${tab}-tab`);
  });
}

// ── SCREENER ──────────────────────────────────────────────────────────────────
async function runScreener(forceRefresh = false) {
  const btn        = document.getElementById('scan-btn');
  const statusBar  = document.getElementById('screener-status');
  const progFill   = document.getElementById('progress-fill');
  const statusTxt  = document.getElementById('status-text');

  btn.disabled = true;
  btn.innerHTML = `<div class="spinner" style="width:14px;height:14px;border-width:2px;margin:0"></div> Scanning...`;

  document.getElementById('screener-results').classList.add('hidden');
  document.getElementById('screener-empty').classList.add('hidden');
  statusBar.classList.remove('hidden');

  // Animate fake progress
  let prog = 0;
  const ticker = setInterval(() => {
    prog = Math.min(prog + Math.random() * 4, 88);
    progFill.style.width = prog + '%';
    statusTxt.textContent = `Scanning ${DEFAULT_UNIVERSE.length} tickers for ADR>5% & Vol>$20M...`;
  }, 600);

  try {
    const res = await fetch(`${API_BASE}/api/screen`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ tickers: DEFAULT_UNIVERSE, force_refresh: forceRefresh }),
    });

    if (!res.ok) throw new Error(`Server error ${res.status}`);
    const data = await res.json();

    clearInterval(ticker);
    progFill.style.width = '100%';
    statusTxt.textContent = `✓ ${data.results.length} stocks passed filters`;

    setTimeout(() => {
      statusBar.classList.add('hidden');
      progFill.style.width = '0%';
    }, 1800);

    screenerCache = data.results;
    document.getElementById('result-count').textContent =
      `${data.results.length} stock${data.results.length !== 1 ? 's' : ''} passed filters · ${data.date}`;

    renderTable(sortData(screenerCache, currentSort));
    document.getElementById('screener-results').classList.remove('hidden');

  } catch (err) {
    clearInterval(ticker);
    statusTxt.textContent = `Error: ${err.message}`;
    progFill.style.width = '0%';
    setTimeout(() => statusBar.classList.add('hidden'), 4000);
    document.getElementById('screener-empty').classList.remove('hidden');
  }

  btn.disabled = false;
  btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg> Run Daily Scan`;
}

// ── TABLE ─────────────────────────────────────────────────────────────────────
function renderTable(rows) {
  const tbody = document.getElementById('screener-body');
  tbody.innerHTML = '';

  rows.forEach((r, i) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="td-rank">${i + 1}</td>
      <td class="td-ticker-name">
        <div class="tk">${r.ticker}</div>
        <div class="nm">${r.name}</div>
      </td>
      <td class="td-price">$${r.price.toFixed(2)}</td>
      <td class="td-score ${scoreColorClass(r.total)}">${r.total}<span style="font-size:11px;font-weight:400;opacity:.45">/100</span></td>
      <td><span class="grade-badge ${gradeClass(r.grade)}">${r.grade}</span></td>
      <td class="td-adr"><span class="adr-pill">${r.adr_pct.toFixed(1)}%</span></td>
      <td class="td-vol">$${fmtVol(r.dollar_volume)}</td>
      <td>${miniScore(r.trend.score, 30)}</td>
      <td>${miniScore(r.momentum.score, 25)}</td>
      <td>${miniScore(r.volatility.score, 25)}</td>
      <td>${miniScore(r.volume.score, 20)}</td>
      <td><button class="expand-btn">Details ↗</button></td>
    `;
    tr.querySelector('.expand-btn').addEventListener('click', e => {
      e.stopPropagation();
      openModal(r);
    });
    tbody.appendChild(tr);
  });
}

function miniScore(score, max) {
  const pct = (score / max * 100).toFixed(0);
  const col = barColor(score / max);
  return `
    <div class="mini-score">
      <div class="mini-track"><div class="mini-fill" style="width:${pct}%;background:${col}"></div></div>
      <span class="mini-num">${score}</span>
    </div>`;
}

// ── RATER ─────────────────────────────────────────────────────────────────────
async function analyzeStock() {
  const ticker = document.getElementById('ticker-input').value.trim().toUpperCase();
  if (!ticker) return;

  const loading = document.getElementById('rater-loading');
  const error   = document.getElementById('rater-error');
  const result  = document.getElementById('rater-result');

  document.getElementById('loading-ticker').textContent = ticker;
  loading.classList.remove('hidden');
  error.classList.add('hidden');
  result.classList.add('hidden');

  try {
    const res = await fetch(`${API_BASE}/api/rate`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ticker }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `Server error ${res.status}`);

    loading.classList.add('hidden');
    result.innerHTML = buildCard(data);
    result.classList.remove('hidden');
    setTimeout(() => animateBars(result), 60);

  } catch (err) {
    loading.classList.add('hidden');
    document.getElementById('error-msg').textContent = err.message;
    error.classList.remove('hidden');
  }
}

// ── CARD BUILDER ──────────────────────────────────────────────────────────────
function buildCard(r) {
  const ringClass = r.total >= 75 ? 'great' : r.total >= 65 ? 'good' : r.total >= 50 ? 'mid' : 'low';
  const barColor2 = r.total >= 75 ? '#10b981' : r.total >= 50 ? '#f59e0b' : '#ef4444';
  const cats = [
    { key: 'trend',      icon: '📈', label: 'Trend Analysis',          max: 30 },
    { key: 'momentum',   icon: '⚡', label: 'Momentum',                 max: 25 },
    { key: 'volatility', icon: '🌀', label: 'Volatility & Compression', max: 25 },
    { key: 'volume',     icon: '📊', label: 'Volume Behavior',          max: 20 },
  ];

  const adrChip = r.adr_pct       ? `<span class="ac-chip">ADR ${r.adr_pct.toFixed(1)}%</span>` : '';
  const volChip = r.dollar_volume ? `<span class="ac-chip">$${fmtVol(r.dollar_volume)} Daily Vol</span>` : '';

  const catsHTML = cats.map(c => {
    const d   = r[c.key];
    const pct = (d.score / c.max * 100).toFixed(1);
    const bc  = barColor(d.score / c.max);
    const bullets = d.conditions.map(s => `<li>${s}</li>`).join('');
    return `
      <div class="cat-panel">
        <div class="cat-top">
          <div class="cat-title">${c.icon} ${c.label}</div>
          <div class="cat-pts">${d.score}/${c.max}</div>
        </div>
        <div class="cat-bar-row">
          <div class="cat-track">
            <div class="cat-fill" style="width:0%;background:${bc}" data-target="${pct}"></div>
          </div>
          <span class="status-chip chip-${d.status}">${d.status}</span>
        </div>
        <ul class="cat-conditions">${bullets}</ul>
      </div>`;
  }).join('');

  return `
    <div class="ac-wrap">
      <div class="ac-header">
        <div class="ac-meta">
          <div class="ac-ticker">${r.ticker}</div>
          <div class="ac-name">${r.name}</div>
          <div class="ac-chips">${adrChip}${volChip}</div>
        </div>
        <div class="ac-price-block">
          <div class="ac-price-label">Last Price</div>
          <div class="ac-price-val">$${r.price.toFixed(2)}</div>
        </div>
        <div class="ac-score-block">
          <div class="score-ring ${ringClass}">
            <span class="score-num">${r.total}</span>
            <span class="score-denom">/100</span>
          </div>
          <span class="grade-badge ${gradeClass(r.grade)}">${r.grade}</span>
        </div>
      </div>
      <div class="ac-verdict">↳ ${r.verdict}</div>
      <div class="ac-bar-row">
        <div class="ac-bar-track">
          <div class="ac-bar-fill" style="width:0%;background:linear-gradient(90deg,${barColor2}99,${barColor2})" data-target="${r.total}"></div>
        </div>
        <div class="bar-ticks"><span>0</span><span>25</span><span>50</span><span>75</span><span>100</span></div>
      </div>
      <div class="ac-grid">${catsHTML}</div>
    </div>`;
}

// ── MODAL ─────────────────────────────────────────────────────────────────────
function openModal(r) {
  const content = document.getElementById('modal-content');
  content.innerHTML = buildCard(r);
  document.getElementById('analysis-modal').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
  setTimeout(() => animateBars(content), 60);
}

function closeModal() {
  document.getElementById('analysis-modal').classList.add('hidden');
  document.body.style.overflow = '';
}

// ── HELPERS ───────────────────────────────────────────────────────────────────
function animateBars(container) {
  container.querySelectorAll('[data-target]').forEach(el => {
    el.style.width = el.dataset.target + '%';
  });
}

function sortData(data, key) {
  const map = {
    score:      r => -r.total,
    adr:        r => -r.adr_pct,
    dolvol:     r => -r.dollar_volume,
    trend:      r => -r.trend.score,
    momentum:   r => -r.momentum.score,
    volatility: r => -r.volatility.score,
    volume:     r => -r.volume.score,
  };
  return [...data].sort((a, b) => map[key](a) - map[key](b));
}

function scoreColorClass(s) {
  if (s >= 75) return 'sc-great';
  if (s >= 65) return 'sc-good';
  if (s >= 50) return 'sc-mid';
  return 'sc-low';
}

function barColor(ratio) {
  if (ratio >= 0.75) return '#10b981';
  if (ratio >= 0.60) return '#34d399';
  if (ratio >= 0.45) return '#f59e0b';
  return '#ef4444';
}

function gradeClass(g) {
  const map = { 'A+': 'gAplus', 'A': 'gA', 'B': 'gB', 'C': 'gC', 'D': 'gD', 'F': 'gF' };
  return map[g] || 'gF';
}

function fmtVol(v) {
  if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(0) + 'K';
  return v.toFixed(0);
}
