/* ═══════════════════════════════════════
   SwingScan — app.js
   Qullamaggie Breakout Screener
═══════════════════════════════════════ */

const API_BASE  = 'https://swingscan.onrender.com';
const DATA_BASE = '../data';

let screenerCache = [];
let currentSort   = 'score';

const CATEGORIES = [
  { key: 'prior_move',    label: 'Prior Move',         max: 25, icon: '🚀' },
  { key: 'consolidation', label: 'Consolidation',      max: 20, icon: '🔒' },
  { key: 'ma_surf',       label: 'MA Surf',            max: 15, icon: '🏄' },
  { key: 'br_ready',      label: 'Breakout Readiness', max: 15, icon: '🎯' },
  { key: 'vol_sig',       label: 'Volume Signature',   max: 15, icon: '📊' },
  { key: 'rel_str',       label: 'Relative Strength',  max: 10, icon: '💪' },
];

// ── INIT ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {

  document.querySelectorAll('.tab-btn').forEach(btn =>
    btn.addEventListener('click', () => switchTab(btn.dataset.tab))
  );

  document.getElementById('sort-select').addEventListener('change', e => {
    currentSort = e.target.value;
    if (screenerCache.length) renderTable(sortData(screenerCache));
  });

  document.getElementById('date-select').addEventListener('change', () => loadScan());
  document.getElementById('scan-btn').addEventListener('click',     () => loadScan());
  document.getElementById('analyze-btn').addEventListener('click',  () => analyzeStock());

  document.getElementById('ticker-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') analyzeStock();
  });

  document.querySelectorAll('.qp-btn').forEach(btn =>
    btn.addEventListener('click', () => {
      document.getElementById('ticker-input').value = btn.dataset.ticker;
      analyzeStock();
    })
  );

  document.querySelector('.modal-close').addEventListener('click', closeModal);
  document.querySelector('.modal-backdrop').addEventListener('click', closeModal);
  document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

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
    p.classList.toggle('hidden', p.id !== `${tab}-tab`);
  });
}

// ── DATE INDEX ────────────────────────────────────────────────────────────────
async function loadDateIndex() {
  try {
    const res   = await fetch(`${DATA_BASE}/index.json?t=${Date.now()}`);
    if (!res.ok) return;
    const idx   = await res.json();
    const dates = idx.dates || [];
    const sel   = document.getElementById('date-select');
    sel.innerHTML = '';
    dates.forEach((d, i) => {
      const opt       = document.createElement('option');
      opt.value       = d;
      opt.textContent = i === 0 ? `${formatDate(d)} (latest)` : formatDate(d);
      sel.appendChild(opt);
    });
    if (dates.length > 0) sel.classList.remove('hidden');
  } catch (_) {}
}

// ── LOAD SCAN ─────────────────────────────────────────────────────────────────
async function loadScan() {
  const sel  = document.getElementById('date-select');
  const date = sel.value;
  const url  = date
    ? `${DATA_BASE}/${date}.json?t=${Date.now()}`
    : `${DATA_BASE}/screener_data.json?t=${Date.now()}`;

  showStatus('Loading scan data...');

  try {
    const res  = await fetch(url);
    if (!res.ok) throw new Error(`No scan found for ${date || 'latest'}`);
    const data = await res.json();

    hideStatus();
    screenerCache = data.results || [];

    document.getElementById('viewing-date').textContent =
      data.date ? formatDate(data.date) : '—';
    document.getElementById('result-count').textContent =
      `${screenerCache.length} stock${screenerCache.length !== 1 ? 's' : ''} passed filters` +
      (data.date ? ` · ${formatDate(data.date)}` : '');

    if (screenerCache.length > 0) {
      renderTable(sortData(screenerCache));
      document.getElementById('screener-results').classList.remove('hidden');
      document.getElementById('screener-empty').classList.add('hidden');
    } else {
      document.getElementById('screener-results').classList.add('hidden');
      document.getElementById('screener-empty').classList.remove('hidden');
      document.querySelector('.empty-title').textContent =
        date ? 'No stocks passed filters on this date' : 'No scan data yet';
    }
  } catch (err) {
    hideStatus();
    document.getElementById('screener-results').classList.add('hidden');
    document.getElementById('screener-empty').classList.remove('hidden');
    document.querySelector('.empty-title').textContent = 'Could not load scan data';
    document.querySelector('.empty-sub').textContent   = err.message;
  }
}

// ── SORT ──────────────────────────────────────────────────────────────────────
function sortData(data) {
  return [...data].sort((a, b) => {
    switch (currentSort) {
      case 'prior_move':    return b.prior_move.score    - a.prior_move.score;
      case 'consolidation': return b.consolidation.score - a.consolidation.score;
      case 'ma_surf':       return b.ma_surf.score       - a.ma_surf.score;
      case 'br_ready':      return b.br_ready.score      - a.br_ready.score;
      case 'vol_sig':       return b.vol_sig.score       - a.vol_sig.score;
      case 'rel_str':       return b.rel_str.score       - a.rel_str.score;
      case 'adr':           return b.adr_pct             - a.adr_pct;
      case 'dolvol':        return b.dollar_volume       - a.dollar_volume;
      default:              return b.total               - a.total;
    }
  });
}

// ── RENDER TABLE ──────────────────────────────────────────────────────────────
function renderTable(data) {
  const tbody = document.getElementById('screener-body');
  tbody.innerHTML = '';

  data.forEach((r, i) => {
    const tr = document.createElement('tr');

    tr.innerHTML = `
      <td class="td-rank">${i + 1}</td>
      <td class="td-ticker">
        <div class="tk">${r.ticker}</div>
        <div class="nm">${r.name || ''}</div>
      </td>
      <td class="td-price">$${r.price.toFixed(2)}</td>
      <td class="td-score ${totalScoreClass(r.total)}">${r.total}<span class="denom">/100</span></td>
      <td><span class="grade ${gradeClass(r.grade)}">${r.grade}</span></td>
      <td><span class="adr-pill">${r.adr_pct.toFixed(1)}%</span></td>
      <td class="td-vol">${formatDolVol(r.dollar_volume)}</td>
      <td>${miniBar(r.prior_move.score,    25)}</td>
      <td>${miniBar(r.consolidation.score, 20)}</td>
      <td>${miniBar(r.ma_surf.score,       15)}</td>
      <td>${miniBar(r.br_ready.score,      15)}</td>
      <td>${miniBar(r.vol_sig.score,       15)}</td>
      <td>${miniBar(r.rel_str.score,       10)}</td>
      <td>
        <button class="expand-btn" data-ticker="${r.ticker}">Details →</button>
      </td>
    `;

    tr.querySelector('.expand-btn').addEventListener('click', e => {
      e.stopPropagation();
      openModal(r);
    });
    tr.addEventListener('click', () => openModal(r));

    tbody.appendChild(tr);
  });
}

// ── ANALYZE STOCK ─────────────────────────────────────────────────────────────
async function analyzeStock() {
  const ticker = document.getElementById('ticker-input').value.trim().toUpperCase();
  if (!ticker) return;

  document.getElementById('rater-result').classList.add('hidden');
  document.getElementById('rater-error').classList.add('hidden');
  document.getElementById('loading-ticker').textContent = ticker;
  document.getElementById('rater-loading').classList.remove('hidden');

  try {
    const res  = await fetch(`${API_BASE}/api/rate`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ticker }),
    });

    const data = await res.json();
    document.getElementById('rater-loading').classList.add('hidden');

    if (!res.ok) {
      document.getElementById('error-msg').textContent = data.error || `Error ${res.status}`;
      document.getElementById('rater-error').classList.remove('hidden');
      return;
    }

    const el = document.getElementById('rater-result');
    el.innerHTML = buildAnalysisCard(data);
    el.classList.remove('hidden');

  } catch (err) {
    document.getElementById('rater-loading').classList.add('hidden');
    document.getElementById('error-msg').textContent = `Network error: ${err.message}`;
    document.getElementById('rater-error').classList.remove('hidden');
  }
}

// ── MODAL ─────────────────────────────────────────────────────────────────────
function openModal(r) {
  document.getElementById('modal-content').innerHTML = buildAnalysisCard(r);
  document.getElementById('modal').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

function closeModal() {
  document.getElementById('modal').classList.add('hidden');
  document.body.style.overflow = '';
}

// ── BUILD ANALYSIS CARD ───────────────────────────────────────────────────────
function buildAnalysisCard(r) {
  const pct       = r.total / 100;
  const ringClass = pct >= 0.75 ? 'great' : pct >= 0.50 ? 'mid' : 'low';
  const barColour = pct >= 0.75 ? '#10b981' : pct >= 0.50 ? '#f59e0b' : '#ef4444';
  const barWidth  = Math.round(pct * 100);

  let html = `
    <div class="ac">
      <div class="ac-head">
        <div class="ac-meta">
          <div class="ac-ticker">${r.ticker}</div>
          <div class="ac-name">${r.name || ''}</div>
          <div class="ac-chips">
            <span class="ac-chip">ADR ${r.adr_pct.toFixed(1)}%</span>
            <span class="ac-chip">${formatDolVol(r.dollar_volume)}/day</span>
          </div>
        </div>
        <div class="ac-price-block">
          <div class="ac-price-label">Last Price</div>
          <div class="ac-price-val">$${r.price.toFixed(2)}</div>
        </div>
        <div class="ac-score-block">
          <div class="score-ring ${ringClass}">
            <div class="score-num">${r.total}</div>
            <div class="score-denom">/100</div>
          </div>
          <span class="grade ${gradeClass(r.grade)}">${r.grade}</span>
        </div>
      </div>

      <div class="ac-verdict">${r.verdict}</div>

      <div class="ac-bar-row">
        <div class="ac-bar-track">
          <div class="ac-bar-fill" style="width:${barWidth}%;background:${barColour};"></div>
        </div>
        <div class="bar-ticks">
          <span>0</span><span>25</span><span>50</span><span>75</span><span>100</span>
        </div>
      </div>

      <div class="ac-grid">
  `;

  CATEGORIES.forEach(cat => {
    const d = r[cat.key];

    if (!d) {
      html += `
        <div class="cat-panel" style="opacity:0.35;">
          <div class="cat-top">
            <div class="cat-title"><span>${cat.icon}</span><span>${cat.label}</span></div>
            <div class="cat-pts">—/${cat.max}</div>
          </div>
          <ul class="cat-conditions">
            <li>Data unavailable — redeploy api_server.py</li>
          </ul>
        </div>
      `;
      return;
    }

    const catPct  = d.score / cat.max;
    const fillClr = catPct >= 0.65 ? '#10b981' : catPct >= 0.35 ? '#f59e0b' : '#ef4444';
    const fillW   = Math.round(catPct * 100);

    html += `
      <div class="cat-panel">
        <div class="cat-top">
          <div class="cat-title">
            <span>${cat.icon}</span>
            <span>${cat.label}</span>
          </div>
          <div class="cat-pts">${d.score}/${cat.max}</div>
        </div>
        <div class="cat-bar-row">
          <div class="cat-track">
            <div class="cat-fill" style="width:${fillW}%;background:${fillClr};"></div>
          </div>
          <span class="chip chip-${d.status}">${d.status}</span>
        </div>
        <ul class="cat-conditions">
          ${(d.conditions || []).map(c => `<li>${c}</li>`).join('')}
        </ul>
      </div>
    `;
  });

  html += `</div></div>`;
  return html;
}

// ── STATUS BAR ────────────────────────────────────────────────────────────────
function showStatus(msg) {
  document.getElementById('screener-status').classList.remove('hidden');
  document.getElementById('status-text').textContent = msg;
  document.getElementById('status-fill').style.width = '40%';
}

function hideStatus() {
  document.getElementById('status-fill').style.width = '100%';
  setTimeout(() => {
    document.getElementById('screener-status').classList.add('hidden');
    document.getElementById('status-fill').style.width = '0%';
  }, 400);
}

// ── HELPERS ───────────────────────────────────────────────────────────────────
function miniBar(score, max) {
  const pct = score / max;
  const clr = pct >= 0.65 ? '#10b981' : pct >= 0.35 ? '#f59e0b' : '#ef4444';
  return `
    <div class="mini">
      <div class="mini-track">
        <div class="mini-fill" style="width:${Math.round(pct * 100)}%;background:${clr};"></div>
      </div>
      <span class="mini-num">${score}</span>
    </div>
  `;
}

function totalScoreClass(score) {
  if (score >= 75) return 'sc-great';
  if (score >= 62) return 'sc-good';
  if (score >= 48) return 'sc-mid';
  return 'sc-low';
}

function gradeClass(grade) {
  const map = { 'A+': 'gAplus', 'A': 'gA', 'B': 'gB', 'C': 'gC', 'D': 'gD', 'F': 'gF' };
  return map[grade] || 'gF';
}

function formatDolVol(v) {
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${(v / 1e3).toFixed(0)}K`;
}

function formatDate(d) {
  if (!d) return '—';
  const [y, m, day] = d.split('-');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[parseInt(m) - 1]} ${parseInt(day)}, ${y}`;
}
