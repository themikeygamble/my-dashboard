/* ═══════════════════════════════════════
   SwingScan — app.js
═══════════════════════════════════════ */

const API_BASE  = 'https://swingscan.onrender.com';
const DATA_BASE = '../data';

let screenerCache  = [];
let currentSort    = 'score';
let availableDates = [];

// ── INIT ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {

  // Tab buttons (in header)
  document.querySelectorAll('.tab-btn').forEach(btn =>
    btn.addEventListener('click', () => switchTab(btn.dataset.tab))
  );

  // Sort dropdown
  document.getElementById('sort-select').addEventListener('change', e => {
    currentSort = e.target.value;
    if (screenerCache.length) renderTable(sortData(screenerCache, currentSort));
  });

  // Date picker
  document.getElementById('date-select').addEventListener('change', () => loadScan());

  // Load scan button
  document.getElementById('scan-btn').addEventListener('click', () => loadScan());

  // Rater — analyze button
  document.getElementById('analyze-btn').addEventListener('click', () => analyzeStock());

  // Rater — enter key
  document.getElementById('ticker-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') analyzeStock();
  });

  // Quick pick buttons
  document.querySelectorAll('.qp-btn').forEach(btn =>
    btn.addEventListener('click', () => {
      document.getElementById('ticker-input').value = btn.dataset.ticker;
      analyzeStock();
    })
  );

  // Modal close
  document.querySelector('.modal-close').addEventListener('click', closeModal);
  document.querySelector('.modal-backdrop').addEventListener('click', closeModal);
  document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

  // On load — fetch date index then load latest scan
  await loadDateIndex();
  loadScan();
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

// ── DATE INDEX ────────────────────────────────────────────────────────────────
async function loadDateIndex() {
  try {
    const res = await fetch(`${DATA_BASE}/index.json?t=${Date.now()}`);
    if (!res.ok) return;
    const idx = await res.json();

    availableDates = idx.dates || [];
    const sel = document.getElementById('date-select');
    sel.innerHTML = '';

    availableDates.forEach((d, i) => {
      const opt = document.createElement('option');
      opt.value = d;
      opt.textContent = i === 0 ? `${formatDate(d)} (latest)` : formatDate(d);
      sel.appendChild(opt);
    });

    if (availableDates.length > 0) {
      sel.classList.remove('hidden');
    }
  } catch (_) {
    // No index yet — first run
  }
}

// ── LOAD SCAN ─────────────────────────────────────────────────────────────────
async function loadScan() {
  const sel          = document.getElementById('date-select');
  const selectedDate = sel.value;
  const url          = selectedDate
    ? `${DATA_BASE}/${selectedDate}.json?t=${Date.now()}`
    : `${DATA_BASE}/screener_data.json?t=${Date.now()}`;

  showStatus('Loading scan data...');

  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`No scan found for ${selectedDate || 'latest'}`);
    const data = await res.json();

    hideStatus();

    screenerCache = data.results || [];

    document.getElementById('viewing-date').textContent =
      data.date ? formatDate(data.date) : '—';

    document.getElementById('result-count').textContent =
      `${screenerCache.length} stock${screenerCache.length !== 1 ? 's' : ''} passed filters` +
      (data.date ? ` · ${formatDate(data.date)}` : '');

    if (screenerCache.length > 0) {
      renderTable(sortData(screenerCache, currentSort));
      document.getElementById('screener-results').classList.remove('hidden');
      document.getElementById('screener-empty').classList.add('hidden');
    } else {
      document.getElementById('screener-results').classList.add('hidden');
      document.getElementById('screener-empty').classList.remove('hidden');
      document.querySelector('.empty-title').textContent =
        data.date ? 'No stocks passed filters on this date' : 'No scan data yet';
    }

  } catch (err) {
    hideStatus();
    document.getElementById('screener-results').classList.add('hidden');
    document.getElementById('screener-empty').classList.remove('hidden');
    document.querySelector('.empty-title').textContent = err.message;
  }
}

// ── STATUS BAR ────────────────────────────────────────────────────────────────
function showStatus(msg) {
  const bar  = document.getElementById('screener-status');
  const fill = document.getElementById('status-fill');
  const txt  = document.getElementById('status-text');

  bar.classList.remove('hidden');
  txt.textContent = msg;

  let p = 0;
  clearInterval(fill._interval);
  fill._interval = setInterval(() => {
    p = Math.min(p + Math.random() * 6, 90);
    fill.style.width = p + '%';
  }, 300);
}

function hideStatus() {
  const bar  = document.getElementById('screener-status');
  const fill = document.getElementById('status-fill');
  clearInterval(fill._interval);
  fill.style.width = '100%';
  document.getElementById('status-text').textContent = 'Done';
  setTimeout(() => {
    bar.classList.add('hidden');
    fill.style.width = '0%';
  }, 600);
}

// ── TABLE ─────────────────────────────────────────────────────────────────────
function renderTable(rows) {
  const tbody = document.getElementById('screener-body');
  tbody.innerHTML = '';

  rows.forEach((r, i) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="td-rank">${i + 1}</td>
      <td class="td-ticker">
        <div class="tk">${r.ticker}</div>
        <div class="nm">${r.name}</div>
      </td>
      <td class="td-price">$${r.price.toFixed(2)}</td>
      <td class="td-score ${scoreColorClass(r.total)}">${r.total}<span class="denom">/100</span></td>
      <td><span class="grade ${gradeClass(r.grade)}">${r.grade}</span></td>
      <td><span class="adr-pill">${r.adr_pct.toFixed(1)}%</span></td>
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
    <div class="mini">
      <div class="mini-track">
        <div class="mini-fill" style="width:${pct}%;background:${col}"></div>
      </div>
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
    const res  = await fetch(`${API_BASE}/api/rate`, {
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

// ── ANALYSIS CARD ─────────────────────────────────────────────────────────────
function buildCard(r) {
  const ringClass = r.total >= 75 ? 'great' : r.total >= 65 ? 'good' : r.total >= 50 ? 'mid' : 'low';
  const barCol    = r.total >= 75 ? '#10b981' : r.total >= 50 ? '#f59e0b' : '#ef4444';

  const cats = [
    { key: 'trend',      icon: '📈', label: 'Trend Analysis',          max: 30 },
    { key: 'momentum',   icon: '⚡', label: 'Momentum',                 max: 25 },
    { key: 'volatility', icon: '🌀', label: 'Volatility & Compression', max: 25 },
    { key: 'volume',     icon: '📊', label: 'Volume Behavior',          max: 20 },
  ];

  const adrChip = r.adr_pct
    ? `<span class="ac-chip">ADR ${r.adr_pct.toFixed(1)}%</span>` : '';
  const volChip = r.dollar_volume
    ? `<span class="ac-chip">$${fmtVol(r.dollar_volume)} Daily Vol</span>` : '';

  const catsHTML = cats.map(c => {
    const d   = r[c.key];
    const pct = (d.score / c.max * 100).toFixed(1);
    const bc  = barColor(d.score / c.max);
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
          <span class="chip chip-${d.status}">${d.status}</span>
        </div>
        <ul class="cat-conditions">
          ${d.conditions.map(s => `<li>${s}</li>`).join('')}
        </ul>
      </div>`;
  }).join('');

  return `
    <div class="ac">
      <div class="ac-head">
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
          <span class="grade ${gradeClass(r.grade)}">${r.grade}</span>
        </div>
      </div>
      <div class="ac-verdict">↳ ${r.verdict}</div>
      <div class="ac-bar-row">
        <div class="ac-bar-track">
          <div class="ac-bar-fill"
               style="width:0%;background:linear-gradient(90deg,${barCol}99,${barCol})"
               data-target="${r.total}"></div>
        </div>
        <div class="bar-ticks">
          <span>0</span><span>25</span><span>50</span><span>75</span><span>100</span>
        </div>
      </div>
      <div class="ac-grid">${catsHTML}</div>
    </div>`;
}

// ── MODAL ─────────────────────────────────────────────────────────────────────
function openModal(r) {
  const content = document.getElementById('modal-content');
  content.innerHTML = buildCard(r);
  document.getElementById('modal').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
  setTimeout(() => animateBars(content), 60);
}

function closeModal() {
  document.getElementById('modal').classList.add('hidden');
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

function formatDate(d) {
  if (!d) return '—';
  const [y, m, day] = d.split('-');
  return new Date(y, m - 1, day).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric'
  });
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
  return { 'A+': 'gAplus', 'A': 'gA', 'B': 'gB', 'C': 'gC', 'D': 'gD', 'F': 'gF' }[g] || 'gF';
}

function fmtVol(v) {
  if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(0) + 'K';
  return v.toFixed(0);
}
