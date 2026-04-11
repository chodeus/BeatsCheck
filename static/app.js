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
let isAuthenticated = false;
let corruptView = localStorage.getItem('beatscheck-corrupt-view') || 'files'; // 'files' or 'albums'

// --- Config metadata for form rendering ---
const CONFIG_SCHEMA = [
  { section: 'Scanning',
    help: 'How and when BeatsCheck scans your music library for corruption.' },
  { key: 'music_dir',        label: 'Scan Location',    type: 'path',   default: '/data',
    desc: 'Folder to scan for audio files' },
  { key: 'mode',             label: 'Scan Mode',        type: 'select', options: ['setup','report','move'], default: 'setup',
    desc: 'setup = idle (no scanning), report = scan and log only, move = quarantine corrupt files' },
  { key: 'workers',          label: 'Workers',          type: 'number', default: '4',
    desc: 'Number of files checked in parallel. More = faster but uses more CPU (2-4 recommended)' },
  { key: 'run_interval',     label: 'Scan Interval',    type: 'number', default: '0',
    desc: 'Hours between automatic scans. 0 = scan once then wait. 24 = daily. 168 = weekly' },
  { key: 'min_file_age',     label: 'Min File Age',     type: 'number', default: '30',
    desc: 'Skip files modified in the last N minutes (avoids flagging active downloads)' },

  { section: 'When Corrupt Files Are Found',
    help: 'What happens after a scan finds corrupt files. By default, corrupt files are only logged — nothing is deleted unless you configure it here or use the Corrupt Files page.' },
  { key: 'delete_after',     label: 'Auto-Delete After',type: 'number', default: '0',
    desc: 'Automatically delete corrupt files after this many days. 0 = never (use Corrupt Files page to delete manually). 7 = one week review window' },
  { key: 'max_auto_delete',  label: 'Safety Limit',     type: 'number', default: '50',
    desc: 'Abort auto-delete if more than this many files would be removed in one run. Prevents mass deletion from filesystem issues. 0 = no limit' },
  { key: 'output_dir',       label: 'Quarantine Folder', type: 'path',  default: '/data/corrupted',
    desc: 'Only for move mode — corrupt files are moved here instead of deleted' },

  { section: 'Lidarr (Automatic Re-download)',
    help: 'Connect to Lidarr so deleted corrupt files are automatically re-downloaded. Monitored albums are re-searched by Lidarr after deletion — no extra config needed.' },
  { key: 'lidarr_url',       label: 'Lidarr URL',       type: 'text',   default: '',
    desc: 'Your Lidarr address, e.g. http://lidarr:8686 or http://192.168.1.100:8686' },
  { key: 'lidarr_api_key',   label: 'API Key',          type: 'password', default: '',
    desc: 'Find this in Lidarr under Settings > General > API Key' },
  { key: 'lidarr_blocklist', label: 'Blocklist',        type: 'select', options: ['false','true'], default: 'false',
    desc: 'Blocklist the corrupt release before deleting so Lidarr downloads a different copy' },
  { key: 'lidarr_search',    label: 'Search Unmonitored', type: 'select', options: ['false','true'], default: 'false',
    desc: 'Queue search for unmonitored albums after auto-delete. Monitored albums are searched automatically by Lidarr' },

  { section: 'Logging' },
  { key: 'log_level',        label: 'Log Level',        type: 'select', options: ['DEBUG','INFO','WARNING','ERROR'], default: 'INFO',
    desc: 'INFO = normal. DEBUG = verbose (for troubleshooting). WARNING/ERROR = quiet' },
  { key: 'max_log_mb',       label: 'Max Log Size (MB)', type: 'number', default: '50',
    desc: 'Rotate log when it exceeds this size. Triggers a fresh full rescan. 0 = never rotate' },

  { section: 'Web Interface' },
  { key: 'webui',      label: 'Enabled',       type: 'select', options: ['false','true'], default: 'false',
    desc: 'Enable the web interface (requires container restart to take effect)' },
  { key: 'webui_port', label: 'Port',           type: 'number', default: '8484',
    desc: 'Port number for the web interface (requires container restart)' },
];

// --- API helpers ---
async function api(path, opts = {}) {
  try {
    const res = await fetch('/api/' + path, {
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    if (res.status === 401) {
      // Session expired — redirect to login
      isAuthenticated = false;
      showAuthPage();
      return null;
    }
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      console.error('API error:', res.status, data.error || res.statusText);
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

// --- Authentication ---
async function checkAuth() {
  try {
    const res = await fetch('/api/auth-status');
    const data = await res.json();
    if (data.setup_required) {
      showPage('setup');
      return;
    }
    if (!data.authenticated) {
      showPage('login');
      return;
    }
    isAuthenticated = true;
    showApp();
  } catch (e) {
    showPage('login');
  }
}

function showPage(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const el = document.getElementById('page-' + page);
  if (el) el.classList.add('active');
  // Hide/show app chrome for auth pages
  const sidebar = document.getElementById('sidebar');
  const logoutBtn = document.getElementById('logout-btn');
  if (page === 'login' || page === 'setup') {
    sidebar.style.display = 'none';
    if (logoutBtn) logoutBtn.style.display = 'none';
    document.body.classList.add('auth-view');
  } else {
    sidebar.style.display = '';
    if (logoutBtn) logoutBtn.style.display = '';
    document.body.classList.remove('auth-view');
  }
}

function showApp() {
  document.body.classList.remove('auth-view');
  const sidebar = document.getElementById('sidebar');
  sidebar.style.display = '';
  const logoutBtn = document.getElementById('logout-btn');
  if (logoutBtn) logoutBtn.style.display = '';
  initRouter();
}

function showAuthPage() {
  stopStatusPoll();
  stopLogPoll();
  checkAuth();
}

async function doSetup(e) {
  e.preventDefault();
  const username = document.getElementById('setup-username').value.trim();
  const password = document.getElementById('setup-password').value;
  const confirm = document.getElementById('setup-confirm').value;
  const error = document.getElementById('setup-error');

  if (!username) { error.textContent = 'Username is required'; return; }
  if (password.length < 4) { error.textContent = 'Password must be at least 4 characters'; return; }
  if (password !== confirm) { error.textContent = 'Passwords do not match'; return; }

  error.textContent = '';
  const btn = e.target.querySelector('[type="submit"]');
  btn.disabled = true;

  try {
    const res = await fetch('/api/setup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const data = await res.json();
    if (res.ok && data.ok) {
      isAuthenticated = true;
      showApp();
    } else {
      error.textContent = data.error || 'Setup failed';
    }
  } catch (err) {
    error.textContent = 'Connection error';
  }
  btn.disabled = false;
}

async function doLogin(e) {
  e.preventDefault();
  const username = document.getElementById('login-username').value.trim();
  const password = document.getElementById('login-password').value;
  const error = document.getElementById('login-error');

  if (!username || !password) { error.textContent = 'Username and password required'; return; }

  error.textContent = '';
  const btn = e.target.querySelector('[type="submit"]');
  btn.disabled = true;

  try {
    const res = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const data = await res.json();
    if (res.ok && data.ok) {
      isAuthenticated = true;
      showApp();
    } else {
      error.textContent = data.error || 'Login failed';
    }
  } catch (err) {
    error.textContent = 'Connection error';
  }
  btn.disabled = false;
}

async function doLogout() {
  await apiPost('logout', {});
  isAuthenticated = false;
  stopStatusPoll();
  stopLogPoll();
  showPage('login');
  // Clear form fields
  const fields = ['login-username', 'login-password'];
  fields.forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
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

  closeSidebar();

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

  document.getElementById('version-badge').textContent = 'v' + (data.version || '?');

  const dot = document.getElementById('status-indicator');
  const stxt = document.getElementById('status-text');
  const status = data.status || 'unknown';
  dot.className = 'status-dot ' + status;
  stxt.textContent = status.charAt(0).toUpperCase() + status.slice(1);

  const summary = data.summary || {};
  setCardValue('dash-status', status.charAt(0).toUpperCase() + status.slice(1));
  setCardValue('dash-mode', (data.mode || '--').toUpperCase());
  setCardValue('dash-uptime', formatUptime(data.uptime));
  setCardValue('dash-corrupt', summary.corrupted != null ? summary.corrupted : '--');
  setCardValue('dash-library', summary.library_size_human || summary.library_files ?
    (summary.library_size_human || summary.library_files + ' files') : '--');
  setCardValue('dash-last-scan', summary.finished || '--');

  const prog = data.scan_progress;
  const section = document.getElementById('scan-progress-section');
  if (prog && status === 'scanning') {
    section.style.display = '';
    const pct = prog.total > 0 ? Math.round((prog.current / prog.total) * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';

    const bar = section.querySelector('.progress-bar');
    if (bar) bar.setAttribute('aria-valuenow', pct);

    setText('progress-text', prog.current + ' / ' + prog.total + ' files (' + pct + '%)');
    const corruptNum = data.corrupt_count || 0;
    setText('progress-corrupt', corruptNum > 0 ? corruptNum + ' corrupt found' : '');
    setText('progress-file', prog.file || '');

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

  // Disable rescan buttons while scanning, show cancel button
  const isScanning = status === 'scanning';
  document.querySelectorAll('.action-bar .btn:not(#cancel-scan-btn)').forEach(b => {
    b.disabled = isScanning;
  });
  const cancelBtn = document.getElementById('cancel-scan-btn');
  if (cancelBtn) {
    cancelBtn.style.display = isScanning ? '' : 'none';
    cancelBtn.disabled = false;
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
  const viewBtn = document.getElementById('view-toggle-btn');
  if (viewBtn) viewBtn.textContent = corruptView === 'files' ? 'Group by Album' : 'Show All Files';
  applyCorruptFilters();

  // Check if scanning — disable deletes and show banner
  const status = await api('status');
  const scanning = status && status.status === 'scanning';
  const banner = document.getElementById('corrupt-scan-banner');
  if (banner) banner.style.display = scanning ? '' : 'none';
  document.querySelectorAll('#page-corrupt .btn-danger').forEach(b => b.disabled = scanning);
  document.querySelectorAll('#page-corrupt .file-check, #select-all').forEach(c => c.disabled = scanning);
}

function toggleCorruptView() {
  corruptView = corruptView === 'files' ? 'albums' : 'files';
  localStorage.setItem('beatscheck-corrupt-view', corruptView);
  const btn = document.getElementById('view-toggle-btn');
  if (btn) btn.textContent = corruptView === 'files' ? 'Group by Album' : 'Show All Files';
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
  if (corruptView === 'albums') {
    renderCorruptAlbums(filtered);
  } else {
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
}

function renderCorruptAlbums(files) {
  const tbody = document.getElementById('corrupt-tbody');
  if (files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No corrupt files found</td></tr>';
    return;
  }
  // Group by parent folder
  const albums = {};
  files.forEach(f => {
    const dir = f.path.split('/').slice(0, -1).join('/');
    if (!albums[dir]) albums[dir] = [];
    albums[dir].push(f);
  });
  const sorted = Object.entries(albums).sort((a, b) => a[0].localeCompare(b[0]));
  let html = '';
  sorted.forEach(([dir, tracks]) => {
    const albumName = dir.split('/').slice(-2).join(' / ');
    const totalSize = tracks.reduce((s, f) => s + (f.size || 0), 0);
    const allMissing = tracks.every(f => f.missing);
    const albumTotal = tracks[0]?.album_total || tracks.length;
    const allCorrupt = tracks.length >= albumTotal;
    const safeDir = escHtml(dir);
    const countLabel = allCorrupt
      ? `All ${tracks.length} files corrupt`
      : `${tracks.length} of ${albumTotal} files corrupt`;
    const hasLidarr = tracks.some(f => f.has_lidarr_id);
    html += `<tr class="album-header ${allCorrupt ? 'all-corrupt' : ''}" onclick="toggleAlbumExpand(this)">
      <td class="col-check"><input type="checkbox" class="album-check" data-dir="${safeDir}" onchange="toggleAlbumSelect(this)" onclick="event.stopPropagation()" aria-label="Select album"></td>
      <td><strong>${escHtml(albumName)}</strong><br><span class="album-count">${countLabel}</span></td>
      <td class="col-size">${formatSize(totalSize)}</td>
      <td class="col-actions album-actions">
        ${hasLidarr ? `<button class="btn btn-primary btn-sm" onclick="event.stopPropagation();deleteAlbumRedownload('${safeDir}')" ${allMissing ? 'disabled' : ''} title="Delete via Lidarr and re-download">Re-download</button>` : ''}
        <button class="btn btn-danger btn-sm" onclick="event.stopPropagation();deleteAlbum('${safeDir}')" ${allMissing ? 'disabled' : ''} title="Delete files permanently">Delete</button>
        <button class="btn btn-outline btn-sm" onclick="event.stopPropagation();ignoreAlbum('${safeDir}')" title="Hide until next scan">Ignore</button>
      </td>
    </tr>`;
    tracks.forEach(f => {
      const name = f.path.split('/').pop();
      const safePath = escHtml(f.path);
      const cls = f.missing ? 'file-missing' : '';
      html += `<tr class="album-file ${cls}" data-album="${safeDir}" style="display:none">
        <td class="col-check"><input type="checkbox" class="file-check" data-path="${safePath}" onchange="updateDeleteBtn()" aria-label="Select ${escHtml(name)}"></td>
        <td><span style="padding-left:1.5rem;color:var(--text-dim);font-size:.82rem">${escHtml(name)}</span>
          <span style="font-size:.78rem;color:var(--text-muted);margin-left:.5rem">${escHtml(f.reason || '')}</span></td>
        <td class="col-size">${f.missing ? 'N/A' : formatSize(f.size)}</td>
        <td class="col-actions"><button class="btn btn-danger btn-sm" onclick="deleteSingle(this)" data-path="${safePath}" ${f.missing ? 'disabled' : ''}>Delete</button></td>
      </tr>`;
    });
  });
  tbody.innerHTML = html;
}

function toggleAlbumExpand(row) {
  const dir = row.querySelector('.album-check')?.dataset.dir;
  if (!dir) return;
  const files = document.querySelectorAll(`tr.album-file[data-album="${dir}"]`);
  const visible = files[0]?.style.display !== 'none';
  files.forEach(f => f.style.display = visible ? 'none' : '');
  row.classList.toggle('expanded', !visible);
}

function toggleAlbumSelect(checkbox) {
  const dir = checkbox.dataset.dir;
  const checked = checkbox.checked;
  document.querySelectorAll(`tr.album-file[data-album="${dir}"] .file-check`).forEach(c => c.checked = checked);
  updateDeleteBtn();
}

async function deleteAlbum(dir) {
  const files = document.querySelectorAll(`tr.album-file[data-album="${dir}"] .file-check`);
  const paths = Array.from(files).map(c => c.dataset.path).filter(Boolean);
  if (!paths.length || !confirm('Permanently delete ' + paths.length + ' corrupt file(s) from this album?')) return;
  const btns = document.querySelectorAll(`tr.album-header .btn`);
  btns.forEach(b => b.disabled = true);
  try {
    const res = await apiPost('delete', { files: paths });
    if (res && res.count > 0) {
      showToast('Deleted ' + res.count + ' file(s)', 'success');
      loadCorrupt();
      refreshDashboard();
    } else {
      showToast('Delete failed' + (res?.errors?.length ? ': ' + res.errors[0].error : ''), 'error');
    }
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

async function deleteAlbumRedownload(dir) {
  const files = document.querySelectorAll(`tr.album-file[data-album="${dir}"] .file-check`);
  const paths = Array.from(files).map(c => c.dataset.path).filter(Boolean);
  if (!paths.length) return;
  if (!confirm('Delete ' + paths.length + ' corrupt file(s) via Lidarr?\n\nLidarr will blocklist the bad release and re-download a clean copy for monitored albums.')) return;
  const btns = document.querySelectorAll(`tr.album-header .btn`);
  btns.forEach(b => b.disabled = true);
  try {
    const res = await apiPost('delete', { files: paths });
    if (res && res.count > 0) {
      showToast('Deleted ' + res.count + ' file(s) — Lidarr will re-download monitored albums', 'success');
      loadCorrupt();
      refreshDashboard();
    } else {
      showToast('Delete failed' + (res?.errors?.length ? ': ' + res.errors[0].error : ''), 'error');
    }
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

async function ignoreAlbum(dir) {
  // Remove all files in this album from corrupt.txt (hide until next scan)
  const files = document.querySelectorAll(`tr.album-file[data-album="${dir}"] .file-check`);
  const paths = Array.from(files).map(c => c.dataset.path).filter(Boolean);
  if (!paths.length) return;
  const res = await apiPost('ignore', { files: paths });
  if (res && res.ok) {
    showToast('Album ignored — will reappear if found corrupt on next scan', 'info');
    loadCorrupt();
  } else {
    showToast('Ignore failed', 'error');
  }
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
      th.setAttribute('aria-sort', sortDirection === 'asc' ? 'ascending' : 'descending');
    } else {
      th.setAttribute('aria-sort', 'none');
    }
  });
}

function renderCorruptTable(files) {
  const tbody = document.getElementById('corrupt-tbody');
  if (files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No corrupt files found</td></tr>';
    return;
  }
  tbody.innerHTML = files.map(f => {
    const cls = f.missing ? 'file-missing' : '';
    const name = f.path.split('/').pop();
    const dir = f.path.split('/').slice(0, -1).join('/');
    const safePath = escHtml(f.path);
    const reason = f.reason ? `<br><span style="font-size:.78rem;color:var(--text-muted)">${escHtml(f.reason)}</span>` : '';
    return `<tr class="${cls}">
      <td class="col-check"><input type="checkbox" class="file-check" data-path="${safePath}" onchange="updateDeleteBtn()" aria-label="Select ${escHtml(name)}"></td>
      <td><div class="file-path" title="${safePath}"><strong>${escHtml(name)}</strong>${reason}<br><span style="color:var(--text-dim);font-size:.75rem">${escHtml(dir)}</span></div></td>
      <td class="col-size">${f.missing ? 'N/A' : formatSize(f.size)}</td>
      <td class="col-actions"><button class="btn btn-danger btn-sm" onclick="deleteSingle(this)" data-path="${safePath}" ${f.missing ? 'disabled' : ''} aria-label="Delete ${escHtml(name)}">Delete</button></td>
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
  el.disabled = true;
  try {
    const res = await apiPost('delete', { files: [path] });
    if (res && res.count > 0) {
      showToast('Deleted ' + res.count + ' file(s)', 'success');
      loadCorrupt();
      refreshDashboard();
    } else {
      showToast('Delete failed' + (res && res.errors && res.errors.length ? ': ' + res.errors[0].error : ''), 'error');
    }
  } finally {
    el.disabled = false;
  }
}

async function deleteSelected() {
  const checks = document.querySelectorAll('.file-check:checked');
  const paths = Array.from(checks).map(c => c.dataset.path).filter(Boolean);
  if (paths.length === 0) return;
  if (!confirm('Delete ' + paths.length + ' file(s)?')) return;
  const btn = document.getElementById('delete-selected-btn');
  btn.disabled = true;
  try {
    const res = await apiPost('delete', { files: paths });
    if (res && res.count > 0) {
      showToast('Deleted ' + res.count + ' file(s)', 'success');
      document.getElementById('select-all').checked = false;
      loadCorrupt();
      refreshDashboard();
    } else {
      showToast('Delete failed', 'error');
    }
  } finally {
    btn.disabled = false;
  }
}

// Path dropdown loader
async function loadPathOptions(selectEl, currentVal) {
  const data = await api('paths');
  if (!data || !data.paths) return;
  // Collect all paths and children
  const allPaths = new Set();
  data.paths.forEach(p => {
    allPaths.add(p.path);
    p.children.forEach(c => allPaths.add(c));
  });
  // Remove current value dupe, add all as options
  const sorted = Array.from(allPaths).sort();
  sorted.forEach(p => {
    if (p === currentVal) return; // already added
    const opt = document.createElement('option');
    opt.value = p;
    opt.textContent = p;
    selectEl.appendChild(opt);
  });
  selectEl.value = currentVal;
}

// --- Configuration ---
function buildConfigSnapshot() {
  const form = document.getElementById('config-form');
  if (!form) return null;
  const fd = new FormData(form);
  const obj = {};
  for (const [k, v] of fd.entries()) obj[k] = String(v);
  return JSON.stringify(obj);
}

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
  configSnapshot = buildConfigSnapshot();
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
      if (item.help) {
        const help = document.createElement('p');
        help.className = 'config-section-help';
        help.textContent = item.help;
        container.appendChild(help);
      }
      return;
    }
    const group = document.createElement('div');
    group.className = 'config-group';
    const label = document.createElement('label');
    label.setAttribute('for', 'cfg-' + item.key);
    label.textContent = item.label;
    label.title = item.desc || '';
    group.appendChild(label);

    if (item.desc) {
      const desc = document.createElement('span');
      desc.className = 'config-desc';
      desc.textContent = item.desc;
      group.appendChild(desc);
    }

    let input;
    if (item.type === 'path') {
      // Path selector — dropdown populated from /api/paths
      input = document.createElement('select');
      input.id = 'cfg-' + item.key;
      input.name = item.key;
      const currentVal = item.key in values ? values[item.key] : (item.default || '');
      // Add current value as first option
      const cur = document.createElement('option');
      cur.value = currentVal;
      cur.textContent = currentVal || '(not set)';
      input.appendChild(cur);
      // Fetch paths async and populate
      loadPathOptions(input, currentVal);
      input.addEventListener('change', updateUnsavedIndicator);
      group.appendChild(input);
    } else if (item.type === 'select') {
      input = document.createElement('select');
      (item.options || []).forEach(opt => {
        const o = document.createElement('option');
        o.value = opt;
        o.textContent = opt;
        input.appendChild(o);
      });
      input.id = 'cfg-' + item.key;
      input.name = item.key;
      input.value = item.key in values ? values[item.key] : (item.default || '');
      input.addEventListener('change', updateUnsavedIndicator);
      group.appendChild(input);
    } else {
      input = document.createElement('input');
      input.type = item.type || 'text';
      if (item.type === 'number') { input.step = 'any'; input.min = '0'; }
      input.id = 'cfg-' + item.key;
      input.name = item.key;
      input.value = item.key in values ? values[item.key] : (item.default || '');
      input.addEventListener('input', updateUnsavedIndicator);
      input.addEventListener('change', updateUnsavedIndicator);
      group.appendChild(input);
    }
    container.appendChild(group);
  });
}

function hasUnsavedConfig() {
  if (!configSnapshot) return false;
  return buildConfigSnapshot() !== configSnapshot;
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
    configSnapshot = buildConfigSnapshot();
    updateUnsavedIndicator();
  } else {
    status.textContent = 'Save failed';
    status.className = 'form-status error';
    showToast('Save failed' + (res && res.error ? ': ' + res.error : ''), 'error');
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

function highlightLogLine(line, isSearchMatch) {
  let html = escHtml(line);
  // Apply syntax highlighting
  LOG_PATTERNS.forEach(p => {
    html = html.replace(p.regex, '<span class="' + p.cls + '">$1</span>');
  });
  // Search matches get a whole-line background highlight (safe — no regex on HTML)
  if (isSearchMatch) {
    html = '<span class="log-highlight-line">' + html + '</span>';
  }
  return html;
}

async function refreshLogs() {
  const lines = document.getElementById('log-lines').value || '500';
  const data = await api('log?lines=' + encodeURIComponent(lines));
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

  if (levelFilter) {
    lines = lines.filter(line => line.includes(levelFilter));
  }

  const hasSearch = searchTerm.length > 0;
  const searchLower = searchTerm.toLowerCase();
  if (hasSearch) {
    lines = lines.filter(line => line.toLowerCase().includes(searchLower));
  }

  viewer.innerHTML = lines.map(line =>
    '<div class="log-line">' + highlightLogLine(line, hasSearch) + '</div>'
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
  // Validate move mode has output directory configured
  if (mode === 'move') {
    const cfg = await api('config');
    if (cfg) {
      const outputDir = (cfg.config || []).find(e => e.key === 'output_dir');
      if (!outputDir || !outputDir.value || outputDir.value === '/corrupted') {
        const hasMount = await api('status');
        // If output_dir is default and not explicitly set, warn the user
        if (!outputDir || !outputDir.value) {
          showToast('Move mode requires an Output Directory. Configure it in Settings first.', 'warning');
          return;
        }
      }
    }
  }
  const btns = document.querySelectorAll('.action-bar .btn');
  btns.forEach(b => b.disabled = true);
  const res = await apiPost('rescan', { mode, fresh });
  btns.forEach(b => b.disabled = false);
  if (res && res.ok) {
    showToast('Scan triggered (' + mode + (fresh ? ', fresh' : '') + ')', 'success');
    setTimeout(refreshDashboard, 1000);
  } else {
    showToast('Scan failed', 'error');
  }
}

async function cancelScan() {
  const btn = document.getElementById('cancel-scan-btn');
  btn.disabled = true;
  const res = await apiPost('cancel', {});
  btn.disabled = false;
  if (res && res.ok) {
    showToast('Scan cancel requested — finishing current files...', 'warning');
  } else {
    showToast('Cancel failed', 'error');
  }
}

// --- Toast notifications ---
const TOAST_DURATIONS = { success: 4000, error: 6000, warning: 5000, info: 4000 };

function showToast(message, type) {
  const container = document.getElementById('toast-container');
  const duration = TOAST_DURATIONS[type] || 4000;

  const t = document.createElement('div');
  t.className = 'toast ' + (type || '');
  t.setAttribute('aria-atomic', 'true');

  const span = document.createElement('span');
  span.textContent = message;
  t.appendChild(span);

  const close = document.createElement('button');
  close.className = 'toast-close';
  close.innerHTML = '&times;';
  close.setAttribute('aria-label', 'Dismiss notification');
  close.onclick = () => { if (t.parentNode) t.remove(); };
  t.appendChild(close);

  const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const prog = document.createElement('div');
  prog.className = 'toast-progress';
  prog.style.width = '100%';
  t.appendChild(prog);

  container.appendChild(t);

  requestAnimationFrame(() => {
    prog.style.transitionDuration = prefersReduced ? '0ms' : duration + 'ms';
    prog.style.width = '0%';
  });

  const timer = setTimeout(() => { if (t.parentNode) t.remove(); }, duration);

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

  document.getElementById('theme-toggle').addEventListener('click', toggleTheme);

  // Auth forms
  const setupForm = document.getElementById('setup-form');
  if (setupForm) setupForm.addEventListener('submit', doSetup);
  const loginForm = document.getElementById('login-form');
  if (loginForm) loginForm.addEventListener('submit', doLogin);
  const logoutBtn = document.getElementById('logout-btn');
  if (logoutBtn) logoutBtn.addEventListener('click', doLogout);

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

  // Start auth check
  checkAuth();
});
