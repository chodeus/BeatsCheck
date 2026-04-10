/* ============================================================
   BeatsCheck WebUI — Frontend Application
   ============================================================ */

// --- State ---
let currentPage = 'dashboard';
let corruptFiles = [];
let pollTimer = null;
let logTimer = null;

// --- Config metadata for form rendering ---
const CONFIG_SCHEMA = [
  { section: 'Scan Settings' },
  { key: 'mode',             label: 'Mode',             type: 'select', options: ['setup','report','move','delete'], desc: 'Scan mode' },
  { key: 'workers',          label: 'Workers',          type: 'number', desc: 'Parallel ffmpeg workers' },
  { key: 'run_interval',     label: 'Run Interval',     type: 'number', desc: 'Hours between scans (0=once)' },
  { key: 'delete_after',     label: 'Delete After',     type: 'number', desc: 'Days before auto-delete (0=never)' },
  { key: 'max_auto_delete',  label: 'Max Auto Delete',  type: 'number', desc: 'Safety threshold (0=no limit)' },
  { key: 'min_file_age',     label: 'Min File Age',     type: 'number', desc: 'Skip files newer than N minutes' },
  { key: 'log_level',        label: 'Log Level',        type: 'select', options: ['DEBUG','INFO','WARNING','ERROR'] },
  { key: 'max_log_mb',       label: 'Max Log MB',       type: 'number', desc: 'Rotate log at N MB (0=never)' },
  { key: 'output_dir',       label: 'Output Dir',       type: 'text',   desc: 'Quarantine path (move mode)' },
  { section: 'Lidarr Integration' },
  { key: 'lidarr_url',       label: 'Lidarr URL',       type: 'text',   desc: 'e.g. http://lidarr:8686' },
  { key: 'lidarr_api_key',   label: 'Lidarr API Key',   type: 'password', desc: 'Settings > General in Lidarr' },
  { key: 'lidarr_search',    label: 'Lidarr Search',    type: 'select', options: ['false','true'], desc: 'Auto-search after delete' },
  { key: 'lidarr_blocklist', label: 'Lidarr Blocklist', type: 'select', options: ['false','true'], desc: 'Blocklist corrupt releases' },
  { section: 'Web UI' },
  { key: 'webui',      label: 'WebUI Enabled', type: 'select', options: ['false','true'], desc: 'Enable web interface (restart required)' },
  { key: 'webui_port', label: 'WebUI Port',    type: 'number', desc: 'Port for web interface (restart required)' },
];

// --- API helpers ---
async function api(path, opts = {}) {
  try {
    const res = await fetch('/api/' + path, {
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    return await res.json();
  } catch (e) {
    console.error('API error:', e);
    return null;
  }
}

function apiPost(path, body) {
  return api(path, { method: 'POST', body: JSON.stringify(body) });
}

// --- Theme ---
function initTheme() {
  const saved = localStorage.getItem('beatscheck-theme') || 'dark';
  document.documentElement.setAttribute('data-theme', saved);
  updateThemeIcon(saved);
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme');
  const next = current === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('beatscheck-theme', next);
  updateThemeIcon(next);
}

function updateThemeIcon(theme) {
  const btn = document.getElementById('theme-toggle');
  btn.textContent = theme === 'dark' ? '\u263E' : '\u2600';
}

// --- Navigation ---
function navigate(page) {
  currentPage = page;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(n => n.classList.remove('active'));
  const el = document.getElementById('page-' + page);
  if (el) el.classList.add('active');
  const nav = document.querySelector(`[data-page="${page}"]`);
  if (nav) nav.classList.add('active');

  // Close mobile sidebar
  closeSidebar();

  // Page-specific init
  if (page === 'corrupt') loadCorrupt();
  if (page === 'config') loadConfig();
  if (page === 'logs') { refreshLogs(); startLogPoll(); }
  else stopLogPoll();
}

function initRouter() {
  window.addEventListener('hashchange', () => {
    const page = location.hash.slice(1) || 'dashboard';
    navigate(page);
  });
  const initial = location.hash.slice(1) || 'dashboard';
  navigate(initial);
}

// --- Mobile sidebar ---
function initSidebar() {
  const toggle = document.getElementById('menu-toggle');
  toggle.addEventListener('click', () => {
    document.getElementById('sidebar').classList.toggle('open');
    getOrCreateOverlay().classList.toggle('visible');
  });
}

function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  const overlay = document.querySelector('.sidebar-overlay');
  if (overlay) overlay.classList.remove('visible');
}

function getOrCreateOverlay() {
  let overlay = document.querySelector('.sidebar-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.className = 'sidebar-overlay';
    overlay.addEventListener('click', closeSidebar);
    document.body.appendChild(overlay);
  }
  return overlay;
}

// --- Dashboard ---
function formatUptime(secs) {
  if (!secs && secs !== 0) return '--';
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return d + 'd ' + h + 'h';
  if (h > 0) return h + 'h ' + m + 'm';
  return m + 'm';
}

function formatSize(bytes) {
  if (!bytes) return '0 B';
  if (bytes >= 1024**4) return (bytes / 1024**4).toFixed(1) + ' TB';
  if (bytes >= 1024**3) return (bytes / 1024**3).toFixed(1) + ' GB';
  if (bytes >= 1024**2) return (bytes / 1024**2).toFixed(1) + ' MB';
  if (bytes >= 1024)    return (bytes / 1024).toFixed(1) + ' KB';
  return bytes + ' B';
}

async function refreshDashboard() {
  const data = await api('status');
  if (!data) return;

  // Version
  document.getElementById('version-badge').textContent = 'v' + (data.version || '?');

  // Status indicator
  const dot = document.getElementById('status-indicator');
  const stxt = document.getElementById('status-text');
  const status = data.status || 'unknown';
  dot.className = 'status-dot ' + status;
  stxt.textContent = status.charAt(0).toUpperCase() + status.slice(1);

  // Cards
  setText('dash-status', status.charAt(0).toUpperCase() + status.slice(1));
  setText('dash-mode', (data.mode || '--').toUpperCase());
  setText('dash-uptime', formatUptime(data.uptime));

  const summary = data.summary || {};
  setText('dash-corrupt', summary.corrupted != null ? summary.corrupted : '--');
  setText('dash-library', summary.library_size_human || summary.library_files ?
    (summary.library_size_human || summary.library_files + ' files') : '--');
  setText('dash-last-scan', summary.finished || '--');

  // Progress
  const prog = data.scan_progress;
  const section = document.getElementById('scan-progress-section');
  if (prog && status === 'scanning') {
    section.style.display = '';
    const pct = prog.total > 0 ? Math.round((prog.current / prog.total) * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';
    setText('progress-text', prog.current + ' / ' + prog.total + ' files (' + pct + '%)');
    setText('progress-file', prog.file || '');
  } else {
    section.style.display = 'none';
  }
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// --- Corrupt Files ---
async function loadCorrupt() {
  const data = await api('corrupt');
  if (!data) return;
  corruptFiles = data.files || [];
  document.getElementById('corrupt-count').textContent = corruptFiles.length;
  renderCorruptTable(corruptFiles);
}

function renderCorruptTable(files) {
  const tbody = document.getElementById('corrupt-tbody');
  if (files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No corrupt files found</td></tr>';
    return;
  }
  tbody.innerHTML = files.map((f, i) => {
    const cls = f.missing ? 'file-missing' : '';
    const name = f.path.split('/').pop();
    const dir = f.path.split('/').slice(0, -1).join('/');
    return `<tr class="${cls}">
      <td class="col-check"><input type="checkbox" class="file-check" data-idx="${i}" onchange="updateDeleteBtn()"></td>
      <td><div class="file-path" title="${escHtml(f.path)}"><strong>${escHtml(name)}</strong><br><span style="color:var(--text-dim);font-size:.75rem">${escHtml(dir)}</span></div></td>
      <td style="font-size:.82rem;color:var(--text-muted)">${escHtml(f.reason || '')}</td>
      <td class="col-size">${formatSize(f.size)}</td>
      <td class="col-action"><button class="btn btn-danger btn-sm" onclick="deleteSingle(${i})" ${f.missing ? 'disabled' : ''}>Delete</button></td>
    </tr>`;
  }).join('');
}

function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// Search filter
document.addEventListener('DOMContentLoaded', () => {
  const search = document.getElementById('corrupt-search');
  if (search) {
    search.addEventListener('input', () => {
      const q = search.value.toLowerCase();
      const filtered = corruptFiles.filter(f =>
        f.path.toLowerCase().includes(q) || (f.reason || '').toLowerCase().includes(q)
      );
      renderCorruptTable(filtered);
    });
  }
});

function toggleSelectAll(el) {
  document.querySelectorAll('.file-check').forEach(c => c.checked = el.checked);
  updateDeleteBtn();
}

function updateDeleteBtn() {
  const any = document.querySelectorAll('.file-check:checked').length > 0;
  document.getElementById('delete-selected-btn').disabled = !any;
}

async function deleteSingle(idx) {
  const f = corruptFiles[idx];
  if (!f || !confirm('Delete ' + f.path + '?')) return;
  const res = await apiPost('delete', { files: [f.path] });
  if (res && res.count > 0) {
    showToast('Deleted ' + res.count + ' file(s)', 'success');
    loadCorrupt();
    refreshDashboard();
  } else {
    showToast('Delete failed', 'error');
  }
}

async function deleteSelected() {
  const checks = document.querySelectorAll('.file-check:checked');
  const paths = Array.from(checks).map(c => corruptFiles[parseInt(c.dataset.idx)]?.path).filter(Boolean);
  if (paths.length === 0) return;
  if (!confirm('Delete ' + paths.length + ' file(s)?')) return;
  const res = await apiPost('delete', { files: paths });
  if (res && res.count > 0) {
    showToast('Deleted ' + res.count + ' file(s)', 'success');
    document.getElementById('select-all').checked = false;
    loadCorrupt();
    refreshDashboard();
  } else {
    showToast('Delete failed', 'error');
  }
}

// --- Configuration ---
async function loadConfig() {
  const data = await api('config');
  if (!data) return;
  const values = {};
  (data.config || []).forEach(e => { values[e.key] = e.value; });
  renderConfigForm(values);
}

function renderConfigForm(values) {
  const container = document.getElementById('config-fields');
  container.innerHTML = '';
  CONFIG_SCHEMA.forEach(item => {
    if (item.section) {
      const title = document.createElement('div');
      title.className = 'config-section-title';
      title.textContent = item.section;
      container.appendChild(title);
      return;
    }
    const group = document.createElement('div');
    group.className = 'config-group';
    const label = document.createElement('label');
    label.setAttribute('for', 'cfg-' + item.key);
    label.textContent = item.key;
    label.title = item.desc || '';
    group.appendChild(label);

    let input;
    if (item.type === 'select') {
      input = document.createElement('select');
      (item.options || []).forEach(opt => {
        const o = document.createElement('option');
        o.value = opt;
        o.textContent = opt;
        input.appendChild(o);
      });
    } else {
      input = document.createElement('input');
      input.type = item.type || 'text';
      if (item.type === 'number') { input.step = 'any'; input.min = '0'; }
    }
    input.id = 'cfg-' + item.key;
    input.name = item.key;
    input.value = values[item.key] || '';
    group.appendChild(input);
    container.appendChild(group);
  });
}

async function saveConfig(e) {
  e.preventDefault();
  const form = document.getElementById('config-form');
  const formData = new FormData(form);
  const config = {};
  for (const [key, val] of formData.entries()) {
    config[key] = val;
  }
  const status = document.getElementById('config-status');
  const res = await apiPost('config', { config });
  if (res && res.ok) {
    status.textContent = 'Saved!';
    status.className = 'form-status';
    showToast('Configuration saved', 'success');
  } else {
    status.textContent = 'Save failed';
    status.className = 'form-status error';
    showToast('Save failed', 'error');
  }
  setTimeout(() => { status.textContent = ''; }, 3000);
}

// --- Logs ---
async function refreshLogs() {
  const data = await api('log?lines=500');
  if (!data) return;
  const viewer = document.getElementById('log-output');
  viewer.textContent = data.log || '(no logs yet)';
  if (document.getElementById('log-autoscroll').checked) {
    viewer.scrollTop = viewer.scrollHeight;
  }
}

function startLogPoll() {
  stopLogPoll();
  logTimer = setInterval(refreshLogs, 5000);
}

function stopLogPoll() {
  if (logTimer) { clearInterval(logTimer); logTimer = null; }
}

// --- Rescan ---
async function triggerRescan(mode, fresh) {
  const res = await apiPost('rescan', { mode, fresh });
  if (res && res.ok) {
    showToast('Rescan triggered (' + mode + (fresh ? ', fresh' : '') + ')', 'success');
    setTimeout(refreshDashboard, 1000);
  } else {
    showToast('Rescan failed', 'error');
  }
}

// --- Toast notifications ---
function showToast(message, type) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();
  const t = document.createElement('div');
  t.className = 'toast ' + (type || '');
  t.textContent = message;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 4000);
}

// --- Polling ---
function startStatusPoll() {
  refreshDashboard();
  pollTimer = setInterval(refreshDashboard, 5000);
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  initSidebar();
  initRouter();
  startStatusPoll();
  document.getElementById('theme-toggle').addEventListener('click', toggleTheme);
});
