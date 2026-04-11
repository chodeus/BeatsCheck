/* ============================================================
   BeatsCheck WebUI — Frontend Application
   Patterns adapted from DAPS experimental frontend
   ============================================================ */

// --- State ---
let currentPage = 'dashboard';
let corruptFiles = [];
let pollTimer = null;
let logTimer = null;
let sortColumn = localStorage.getItem('beatscheck-sort-col') || null;
let sortDirection = localStorage.getItem('beatscheck-sort-dir') || 'asc';
let configSnapshot = null;  // tracks unsaved changes
let scanStartTime = null;
let scanStartCount = 0;
let logRawLines = [];  // unfiltered log lines for client-side filtering

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
    if (!res.ok) {
      console.error('API error:', res.status, res.statusText);
      return null;
    }
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
  // Warn about unsaved config changes
  if (currentPage === 'config' && page !== 'config' && hasUnsavedConfig()) {
    if (!confirm('You have unsaved configuration changes. Leave anyway?')) return;
  }

  currentPage = page;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(n => n.classList.remove('active'));
  const el = document.getElementById('page-' + page);
  if (el) el.classList.add('active');
  const nav = document.querySelector(`[data-page="${page}"]`);
  if (nav) nav.classList.add('active');

  // Close mobile sidebar
  closeSidebar();

  // Page-specific init + polling control
  if (page === 'dashboard') { startStatusPoll(); }
  else { stopStatusPoll(); }

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

  // Keyboard nav for sidebar
  const sidebar = document.getElementById('sidebar');
  sidebar.addEventListener('keydown', (e) => {
    const links = Array.from(sidebar.querySelectorAll('.nav-link'));
    const idx = links.indexOf(document.activeElement);
    if (idx === -1) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      links[(idx + 1) % links.length].focus();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      links[(idx - 1 + links.length) % links.length].focus();
    }
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

let prevCardValues = {};

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

  // Cards with change animation
  const summary = data.summary || {};
  setCardValue('dash-status', status.charAt(0).toUpperCase() + status.slice(1));
  setCardValue('dash-mode', (data.mode || '--').toUpperCase());
  setCardValue('dash-uptime', formatUptime(data.uptime));
  setCardValue('dash-corrupt', summary.corrupted != null ? summary.corrupted : '--');
  setCardValue('dash-library', summary.library_size_human || summary.library_files ?
    (summary.library_size_human || summary.library_files + ' files') : '--');
  setCardValue('dash-last-scan', summary.finished || '--');

  // Progress with ETA
  const prog = data.scan_progress;
  const section = document.getElementById('scan-progress-section');
  if (prog && status === 'scanning') {
    section.style.display = '';
    const pct = prog.total > 0 ? Math.round((prog.current / prog.total) * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';

    // Update ARIA
    const bar = section.querySelector('.progress-bar');
    if (bar) bar.setAttribute('aria-valuenow', pct);

    setText('progress-text', prog.current + ' / ' + prog.total + ' files (' + pct + '%)');
    setText('progress-file', prog.file || '');

    // ETA calculation
    if (!scanStartTime || scanStartCount > prog.current) {
      scanStartTime = Date.now();
      scanStartCount = prog.current;
    }
    const elapsed = (Date.now() - scanStartTime) / 1000;
    const done = prog.current - scanStartCount;
    if (done > 10 && prog.total > prog.current) {
      const rate = done / elapsed;
      const remaining = (prog.total - prog.current) / rate;
      const eta = formatUptime(Math.round(remaining));
      const rateStr = Math.round(rate * 60);
      setText('progress-eta', rateStr + ' files/min \u2022 ~' + eta + ' remaining');
    } else {
      setText('progress-eta', 'Calculating...');
    }
  } else {
    section.style.display = 'none';
    scanStartTime = null;
  }
}

function setCardValue(id, val) {
  const el = document.getElementById(id);
  if (!el) return;
  const strVal = String(val);
  if (prevCardValues[id] !== undefined && prevCardValues[id] !== strVal) {
    el.classList.add('changed');
    setTimeout(() => el.classList.remove('changed'), 600);
  }
  prevCardValues[id] = strVal;
  el.textContent = val;
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// --- Corrupt Files ---
async function loadCorrupt() {
  const data = await api('corrupt');
  if (!data) {
    document.getElementById('corrupt-tbody').innerHTML =
      '<tr><td colspan="5" class="empty-state">Failed to load data</td></tr>';
    return;
  }
  corruptFiles = data.files || [];
  document.getElementById('corrupt-count').textContent = corruptFiles.length;
  applyCorruptFilters();
}

function applyCorruptFilters() {
  const q = (document.getElementById('corrupt-search').value || '').toLowerCase();
  let filtered = corruptFiles;
  if (q) {
    filtered = filtered.filter(f =>
      f.path.toLowerCase().includes(q) || (f.reason || '').toLowerCase().includes(q)
    );
  }
  if (sortColumn) {
    filtered = [...filtered].sort((a, b) => {
      let va, vb;
      if (sortColumn === 'path') { va = a.path; vb = b.path; }
      else if (sortColumn === 'reason') { va = a.reason || ''; vb = b.reason || ''; }
      else if (sortColumn === 'size') { va = a.size || 0; vb = b.size || 0; }
      else return 0;
      if (typeof va === 'string') {
        const cmp = va.localeCompare(vb);
        return sortDirection === 'asc' ? cmp : -cmp;
      }
      return sortDirection === 'asc' ? va - vb : vb - va;
    });
  }
  renderCorruptTable(filtered);
  updateSortIndicators();
}

function sortTable(col) {
  if (sortColumn === col) {
    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
  } else {
    sortColumn = col;
    sortDirection = 'asc';
  }
  localStorage.setItem('beatscheck-sort-col', sortColumn);
  localStorage.setItem('beatscheck-sort-dir', sortDirection);
  applyCorruptFilters();
}

function updateSortIndicators() {
  document.querySelectorAll('thead th.sortable').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.sort === sortColumn) {
      th.classList.add('sort-' + sortDirection);
    }
  });
}

function renderCorruptTable(files) {
  const tbody = document.getElementById('corrupt-tbody');
  if (files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No corrupt files found</td></tr>';
    return;
  }
  tbody.innerHTML = files.map(f => {
    const cls = f.missing ? 'file-missing' : '';
    const name = f.path.split('/').pop();
    const dir = f.path.split('/').slice(0, -1).join('/');
    const safePath = escHtml(f.path);
    return `<tr class="${cls}">
      <td class="col-check"><input type="checkbox" class="file-check" data-path="${safePath}" onchange="updateDeleteBtn()" aria-label="Select ${escHtml(name)}"></td>
      <td><div class="file-path" title="${safePath}"><strong>${escHtml(name)}</strong><br><span style="color:var(--text-dim);font-size:.75rem">${escHtml(dir)}</span></div></td>
      <td style="font-size:.82rem;color:var(--text-muted)">${escHtml(f.reason || '')}</td>
      <td class="col-size">${f.missing ? 'N/A' : formatSize(f.size)}</td>
      <td class="col-action"><button class="btn btn-danger btn-sm" onclick="deleteSingle(this)" data-path="${safePath}" ${f.missing ? 'disabled' : ''} aria-label="Delete ${escHtml(name)}">Delete</button></td>
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
    let debounceTimer;
    search.addEventListener('input', () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(applyCorruptFilters, 150);
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

async function deleteSingle(el) {
  const path = el.dataset.path;
  if (!path || !confirm('Delete ' + path + '?')) return;
  const res = await apiPost('delete', { files: [path] });
  if (res && res.count > 0) {
    showToast('Deleted ' + res.count + ' file(s)', 'success');
    loadCorrupt();
    refreshDashboard();
  } else {
    showToast('Delete failed' + (res && res.errors && res.errors.length ? ': ' + res.errors[0].error : ''), 'error');
  }
}

async function deleteSelected() {
  const checks = document.querySelectorAll('.file-check:checked');
  const paths = Array.from(checks).map(c => c.dataset.path).filter(Boolean);
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
  if (!data) {
    document.getElementById('config-fields').innerHTML =
      '<div class="empty-state">Failed to load configuration</div>';
    return;
  }
  const values = {};
  (data.config || []).forEach(e => { values[e.key] = e.value; });
  renderConfigForm(values);
  configSnapshot = JSON.stringify(values);
  updateUnsavedIndicator();
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

    if (item.desc) {
      const desc = document.createElement('span');
      desc.className = 'config-desc';
      desc.textContent = item.desc;
      group.appendChild(desc);
    }

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
    input.value = item.key in values ? values[item.key] : '';
    input.addEventListener('input', updateUnsavedIndicator);
    input.addEventListener('change', updateUnsavedIndicator);
    group.appendChild(input);
    container.appendChild(group);
  });
}

function hasUnsavedConfig() {
  if (!configSnapshot) return false;
  const form = document.getElementById('config-form');
  if (!form) return false;
  const formData = new FormData(form);
  const current = {};
  for (const [key, val] of formData.entries()) { current[key] = val; }
  return JSON.stringify(current) !== configSnapshot;
}

function updateUnsavedIndicator() {
  const navLink = document.querySelector('[data-page="config"]');
  if (!navLink) return;
  let dot = navLink.querySelector('.unsaved-dot');
  if (hasUnsavedConfig()) {
    if (!dot) {
      dot = document.createElement('span');
      dot.className = 'unsaved-dot';
      dot.title = 'Unsaved changes';
      navLink.appendChild(dot);
    }
  } else {
    if (dot) dot.remove();
  }
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
  const submitBtn = form.querySelector('[type="submit"]');
  submitBtn.disabled = true;
  const res = await apiPost('config', { config });
  submitBtn.disabled = false;
  if (res && res.ok) {
    status.textContent = 'Saved!';
    status.className = 'form-status';
    showToast('Configuration saved', 'success');
    configSnapshot = JSON.stringify(config);
    updateUnsavedIndicator();
  } else {
    status.textContent = 'Save failed';
    status.className = 'form-status error';
    showToast('Save failed', 'error');
  }
  setTimeout(() => { status.textContent = ''; }, 3000);
}

// --- Logs ---
const LOG_PATTERNS = [
  { regex: /\b(CRITICAL)\b/g,  cls: 'log-level-critical' },
  { regex: /\b(ERROR)\b/g,     cls: 'log-level-error' },
  { regex: /\b(WARNING)\b/g,   cls: 'log-level-warning' },
  { regex: /\b(INFO)\b/g,      cls: 'log-level-info' },
  { regex: /\b(DEBUG)\b/g,     cls: 'log-level-debug' },
  { regex: /(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)/g, cls: 'log-timestamp' },
  { regex: /(https?:\/\/\S+)/g, cls: 'log-url' },
  { regex: /(\/(?:[\w.-]+\/)+[\w.-]+)/g, cls: 'log-path' },
  { regex: /\b(\d+(?:\.\d+)?)\s*(?:files?|MB|GB|KB|TB|bytes?|%|ms|seconds?|minutes?|hours?)\b/g, cls: 'log-number' },
];

function highlightLogLine(line, searchTerm) {
  let html = escHtml(line);
  // Apply syntax highlighting
  LOG_PATTERNS.forEach(p => {
    html = html.replace(p.regex, '<span class="' + p.cls + '">$1</span>');
  });
  // Apply search highlight
  if (searchTerm) {
    const escaped = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp('(' + escaped + ')', 'gi');
    html = html.replace(re, '<span class="log-highlight">$1</span>');
  }
  return html;
}

async function refreshLogs() {
  const lines = document.getElementById('log-lines').value || '500';
  const data = await api('log?lines=' + lines);
  if (!data) {
    document.getElementById('log-output').innerHTML = '<span class="log-level-error">(failed to load logs)</span>';
    return;
  }
  logRawLines = (data.log || '').split('\n');
  renderLogOutput();
}

function renderLogOutput() {
  const viewer = document.getElementById('log-output');
  const levelFilter = document.getElementById('log-level-filter').value;
  const searchTerm = document.getElementById('log-search').value.trim();

  let lines = logRawLines;

  // Filter by level
  if (levelFilter) {
    lines = lines.filter(line => line.includes(levelFilter));
  }

  // Filter by search term
  if (searchTerm) {
    const q = searchTerm.toLowerCase();
    lines = lines.filter(line => line.toLowerCase().includes(q));
  }

  // Render with syntax highlighting
  viewer.innerHTML = lines.map(line =>
    '<div class="log-line">' + highlightLogLine(line, searchTerm) + '</div>'
  ).join('');

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

function copyLogs() {
  const text = logRawLines.join('\n');
  navigator.clipboard.writeText(text).then(
    () => showToast('Logs copied to clipboard', 'success'),
    () => showToast('Failed to copy logs', 'error')
  );
}

function downloadLogs() {
  const text = logRawLines.join('\n');
  const blob = new Blob([text], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'beatscheck-' + new Date().toISOString().slice(0, 10) + '.log';
  a.click();
  URL.revokeObjectURL(url);
  showToast('Log file downloaded', 'info');
}

// --- Rescan ---
async function triggerRescan(mode, fresh) {
  const btns = document.querySelectorAll('.action-bar .btn');
  btns.forEach(b => b.disabled = true);
  const res = await apiPost('rescan', { mode, fresh });
  btns.forEach(b => b.disabled = false);
  if (res && res.ok) {
    showToast('Rescan triggered (' + mode + (fresh ? ', fresh' : '') + ')', 'success');
    setTimeout(refreshDashboard, 1000);
  } else {
    showToast('Rescan failed', 'error');
  }
}

// --- Toast notifications ---
const TOAST_DURATIONS = { success: 4000, error: 6000, warning: 5000, info: 4000 };

function showToast(message, type) {
  const container = document.getElementById('toast-container');
  const duration = TOAST_DURATIONS[type] || 4000;

  const t = document.createElement('div');
  t.className = 'toast ' + (type || '');

  // Message text
  const span = document.createElement('span');
  span.textContent = message;
  t.appendChild(span);

  // Close button
  const close = document.createElement('button');
  close.className = 'toast-close';
  close.innerHTML = '&times;';
  close.setAttribute('aria-label', 'Dismiss notification');
  close.onclick = () => { if (t.parentNode) t.remove(); };
  t.appendChild(close);

  // Progress bar
  const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const prog = document.createElement('div');
  prog.className = 'toast-progress';
  prog.style.width = '100%';
  t.appendChild(prog);

  container.appendChild(t);

  // Animate progress bar
  requestAnimationFrame(() => {
    prog.style.transitionDuration = prefersReduced ? '0ms' : duration + 'ms';
    prog.style.width = '0%';
  });

  // Auto-dismiss
  const timer = setTimeout(() => { if (t.parentNode) t.remove(); }, duration);

  // Pause on hover
  t.addEventListener('mouseenter', () => {
    clearTimeout(timer);
    prog.style.transitionDuration = '0ms';
  });
  t.addEventListener('mouseleave', () => {
    const remaining = (parseFloat(getComputedStyle(prog).width) / t.offsetWidth) * duration;
    prog.style.transitionDuration = remaining + 'ms';
    prog.style.width = '0%';
    setTimeout(() => { if (t.parentNode) t.remove(); }, remaining);
  });

  // Cap at 5 toasts
  while (container.children.length > 5) {
    container.removeChild(container.firstChild);
  }
}

// --- Polling ---
function startStatusPoll() {
  stopStatusPoll();
  refreshDashboard();
  pollTimer = setInterval(refreshDashboard, 5000);
}

function stopStatusPoll() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  initSidebar();
  initRouter();
  document.getElementById('theme-toggle').addEventListener('click', toggleTheme);

  // Log controls
  const logLines = document.getElementById('log-lines');
  const logLevel = document.getElementById('log-level-filter');
  const logSearch = document.getElementById('log-search');
  if (logLines) logLines.addEventListener('change', () => { if (currentPage === 'logs') refreshLogs(); });
  if (logLevel) logLevel.addEventListener('change', renderLogOutput);
  if (logSearch) {
    let debounce;
    logSearch.addEventListener('input', () => {
      clearTimeout(debounce);
      debounce = setTimeout(renderLogOutput, 200);
    });
  }
});
