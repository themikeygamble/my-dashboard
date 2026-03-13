/* ═══════════════════════════════════════
   SwingScan — app.js
═══════════════════════════════════════ */

const API_BASE  = 'https://swingscan.onrender.com';
const DATA_BASE = '../data';

let screenerCache  = [];
let currentSort    = 'score';

// ── INIT ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {

  document.querySelectorAll('.tab-btn').forEach(btn =>
    btn.addEventListener('click', () => switchTab(btn.dataset.tab))
  );

  document.getElementById('sort-select').addEventListener('change', e => {
    currentSort = e.target.value;
    if (screenerCache.length) renderTable(sortData(screenerCache, currentSort));
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
    p.classList.toggle('hidden',  p.id !== `${tab}-tab`);
  });
}

// ── DATE INDEX ────────────────────────────────────────────────────────────────
async function loadDateIndex() {
  try {
    const res = await fetch(`${DATA_BASE}/index.json?t=${Date.now()}`);
    if (!res.ok) return;
    const idx  = await res.json();
    const dates = idx.dates || [];
    const sel   = document.getElementById('date-select');
    sel.innerHTML = '';
    dates.forEach((d, i) => {
      const opt = document.createElement('option');
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
      renderTable(sortData(screenerCache, currentSort));
      document.getElementById('screener-results').classList.remove('hidden');
      document.getElementById('screener-empty').classList.add('hidden');
    } else {
      document.getElementById('screener-results').classList.add('hidden');
      document.getElementById('screener-empty').classList.remove('hidden');
      document.querySelector('.empty-title').textContent =
        date ? 'No stocks passed filters on this date' : 'No scan
