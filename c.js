const io = require('socket.io-client');
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

const ROOT_DIR = __dirname;
const CLIENT_ID_PATH = path.resolve(ROOT_DIR, 'client_id.txt');
const RECONNECT_INTERVAL_MS = 5000;
const MAX_RECONNECT_ATTEMPTS = Infinity;
const HEARTBEAT_INTERVAL_MS = 30000;

let clientId = null;
let socket = null;
let currentUrl = null;
let cwd = ROOT_DIR;
let env = { ...process.env };

let jobCounter = 0;
let reconnectAttempts = 0;
let reconnectTimer = null;
let heartbeatTimer = null;
let isConnecting = false;
let manuallyDisconnected = false;
let connectionEpoch = 0;

const jobs = new Map();
let activeUploads = {};

function safeReadFile(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    const stat = fs.statSync(filePath);
    if (!stat.isFile()) return null;
    return fs.readFileSync(filePath, 'utf8').trim();
  } catch {
    return null;
  }
}

function safeWriteFileAtomic(filePath, content) {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const tmp = path.join(dir, `.${base}.${process.pid}.${Date.now()}.tmp`);
  try {
    fs.writeFileSync(tmp, content);
    fs.renameSync(tmp, filePath);
  } catch (err) {
    console.error('Failed to write atomic file:', err.message);
  }
}

function loadClientId() {
  return safeReadFile(CLIENT_ID_PATH) || null;
}

function saveClientId(id) {
  if (!id) return;
  clientId = id;
  try {
    safeWriteFileAtomic(CLIENT_ID_PATH, `${id}\n`);
  } catch (err) {
    console.error('Failed to save client ID:', err.message);
  }
}

function normalizeUrl(url) {
  if (!url) return null;
  url = url.trim();
  if (url.startsWith('localhost:') || url.match(/^\d+\.\d+\.\d+\.\d+:\d+/)) {
    return 'http://' + url;
  }
  if (url.startsWith('http://') || url.startsWith('https://') ||
      url.startsWith('ws://') || url.startsWith('wss://')) {
    return url;
  }
  return 'https://' + url;
}

function expandEnvVars(str) {
  if (process.platform === 'win32') {
    return str.replace(/%([^%]+)%/g, (_, varname) => {
      const val = env[varname.toUpperCase()];
      return val !== undefined ? val : `%${varname}%`;
    });
  }
  return str.replace(/\$(\w+)|\$\{(\w+)\}/g, (_, v1, v2) => {
    const key = v1 || v2;
    const val = env[key];
    return val !== undefined ? val : `$${key}`;
  });
}

function emitSystem(msg) {
  if (socket && socket.connected) socket.emit('system', msg);
  console.log('[System] ' + msg);
}
function emitOutput(msg) {
  if (socket && socket.connected) socket.emit('output', msg);
  console.log('[Output] ' + msg);
}
function emitError(msg) {
  if (socket && socket.connected) socket.emit('error', msg);
  console.error('[Error] ' + msg);
}
function emitDirectory() {
  if (socket && socket.connected) socket.emit('directory', cwd);
}

function connectToServer(url) {
  if (!url) return;
  if (isConnecting) {
    console.log('[Client] Already connecting, waiting...');
    return;
  }
  manuallyDisconnected = false;
  isConnecting = true;
  const normalizedUrl = normalizeUrl(url);
  if (!normalizedUrl) {
    console.error('[Client] Invalid server URL:', url);
    isConnecting = false;
    return;
  }

  console.log(`[Client] Attempting to connect to ${normalizedUrl} (attempt ${reconnectAttempts + 1})`);

  if (socket) {
    try {
      socket.removeAllListeners();
      socket.disconnect();
    } catch (e) {}
    socket = null;
  }

  connectionEpoch = Date.now();

  socket = io(normalizedUrl, {
    transports: ['websocket', 'polling'],
    reconnection: false,
    pingInterval: 25000,
    pingTimeout: 60000,
    secure: normalizedUrl.startsWith('https://') || normalizedUrl.startsWith('wss://'),
    rejectUnauthorized: false,
    forceNew: true,
    timeout: 30000
  });

  socket.on('connect', () => {
    isConnecting = false;
    reconnectAttempts = 0;
    console.log(`[Client] Connected to ${normalizedUrl}`);
    emitSystem(`Connected to server`);
    // Permanent UUID is sent here; server reuses it if valid.
    socket.emit('register-client', { clientId: clientId, epoch: connectionEpoch });
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      if (socket && socket.connected) socket.emit('heartbeat');
    }, HEARTBEAT_INTERVAL_MS);
  });

  socket.on('registered', (uuid) => {
    saveClientId(uuid);   // <-- persistent UUID written to client_id.txt
    console.log(`[Client] Registered as ${uuid}`);
    emitSystem(`Registered as ${uuid}`);
    emitDirectory();
  });

  socket.on('run-command', (cmd) => {
    if (!cmd || typeof cmd !== 'string') { emitError('Invalid command payload'); return; }
    const id = ++jobCounter;
    const trimmed = cmd.trim();
    emitSystem(`[${id}] Starting: ${trimmed}`);
    if (/^cd(?:\s+.*)?$/i.test(trimmed)) { handleCdCommand(id, trimmed); return; }
    runCommand(id, trimmed);
  });

  socket.on('disconnect', (reason) => {
    isConnecting = false;
    console.warn(`[Client] Disconnected: ${reason}`);
    if (!manuallyDisconnected) scheduleReconnect();
  });

  socket.on('connect_error', (err) => {
    isConnecting = false;
    console.error(`[Client] Connection error: ${err.message}`);
    if (!manuallyDisconnected) scheduleReconnect();
  });

  socket.on('error', (err) => {
    console.error(`[Client] Socket error:`, err);
  });

  // ---- file upload handlers ----
  socket.on('file-upload-start', (meta) => {
    console.log(`[Client] Starting file upload: ${meta.name} (${meta.size} bytes, ${meta.totalChunks} chunks)`);
    activeUploads[meta.uploadId] = {
      name: meta.name,
      size: meta.size,
      totalChunks: meta.totalChunks,
      chunks: {},
      received: 0,
      destPath: path.join(cwd, meta.name)
    };
    emitSystem(`Receiving file: ${meta.name} (${meta.size} bytes)`);
  });

  socket.on('file-upload-chunk', (data) => {
    const upload = activeUploads[data.uploadId];
    if (!upload) return;
    upload.chunks[data.index] = data.chunk;
    upload.received++;
    const pct = Math.round((upload.received / upload.totalChunks) * 100);
    if (socket && socket.connected) {
      socket.emit('upload-progress', { name: upload.name, pct });
    }
  });

  socket.on('file-upload-end', (data) => {
    const upload = activeUploads[data.uploadId];
    if (!upload) { socket.emit('file-upload-error', 'Unknown upload ID: ' + data.uploadId); return; }
    try {
      let binary = '';
      for (let i = 0; i < upload.totalChunks; i++) if (upload.chunks[i]) binary += upload.chunks[i];
      const buffer = Buffer.from(binary, 'base64');
      fs.writeFileSync(upload.destPath, buffer);
      const writtenSize = buffer.length;
      console.log(`[Client] File saved: ${upload.destPath} (${writtenSize} bytes)`);
      emitSystem(`File saved: ${upload.destPath} (${writtenSize} bytes)`);
      if (socket && socket.connected) {
        socket.emit('file-uploaded', { name: upload.name, size: writtenSize, path: upload.destPath });
      }
    } catch (err) {
      console.error(`[Client] Failed to write file: ${err.message}`);
      emitError(`Failed to write file: ${err.message}`);
      if (socket && socket.connected) socket.emit('file-upload-error', err.message);
    }
    delete activeUploads[data.uploadId];
  });

  // ---- file download handlers ----
  socket.on('file-download-request', (data) => {
    const filePath = data.path;
    console.log(`[Client] Download request for: ${filePath}`);
    try {
      if (!fs.existsSync(filePath)) { socket.emit('file-download-error', 'File not found: ' + filePath); return; }
      const stat = fs.statSync(filePath);
      if (!stat.isFile()) { socket.emit('file-download-error', 'Not a file: ' + filePath); return; }

      const fileName = path.basename(filePath);
      const fileSize = stat.size;
      const buffer = fs.readFileSync(filePath);
      const base64 = buffer.toString('base64');
      const chunkSize = 64 * 1024;
      const totalChunks = Math.ceil(base64.length / chunkSize);

      socket.emit('file-download-start', {
        name: fileName, size: fileSize, totalChunks: totalChunks, path: filePath
      });

      let sent = 0;
      function sendChunk() {
        if (!socket || !socket.connected) {
          console.error('[Client] Socket disconnected during download');
          return;
        }
        if (sent >= totalChunks) {
          socket.emit('file-download-end', { name: fileName });
          console.log(`[Client] Download complete: ${fileName}`);
          return;
        }
        const chunk = base64.substring(sent * chunkSize, (sent + 1) * chunkSize);
        socket.emit('file-download-chunk', { index: sent, chunk });
        sent++;
        setImmediate(sendChunk);
      }
      sendChunk();
    } catch (err) {
      console.error(`[Client] Download error: ${err.message}`);
      socket.emit('file-download-error', err.message);
    }
  });
}

function scheduleReconnect() {
  if (manuallyDisconnected) return;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  const delay = Math.min(1000 * Math.pow(1.5, Math.min(reconnectAttempts, 10)), 30000);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    reconnectAttempts++;
    if (currentUrl) connectToServer(currentUrl);
  }, delay);
  console.log(`[Client] Reconnect scheduled in ${delay}ms (attempt ${reconnectAttempts + 1})`);
}

function handleCdCommand(id, cmd) {
  const match = cmd.match(/^cd(?:\s+(.+))?$/i);
  let target = match?.[1];
  if (!target) target = os.homedir();
  else { target = expandEnvVars(target).trim().replace(/^["']|["']$/g, ''); }
  const newDir = path.isAbsolute(target) ? path.resolve(target) : path.resolve(cwd, target);
  try {
    const stat = fs.statSync(newDir);
    if (!stat.isDirectory()) throw new Error('not a directory');
    cwd = newDir;
    emitDirectory();
    emitOutput(`[${id}] Changed directory to ${cwd}`);
    emitSystem(`[${id}] Completed`);
  } catch {
    emitError(`[${id}] The system cannot find the path specified: ${target}`);
    emitSystem(`[${id}] Failed`);
  }
}

function killJob(jobId, reason) {
  const job = jobs.get(jobId);
  if (!job) return;
  const { proc, killTimer } = job;
  if (killTimer) clearTimeout(killTimer);
  try {
    if (proc && !proc.killed) {
      proc.kill('SIGTERM');
      const forceKillTimeout = setTimeout(() => {
        try { if (proc && !proc.killed) proc.kill('SIGKILL'); } catch {}
      }, 1500);
      forceKillTimeout.unref?.();
    }
  } catch {}
  jobs.delete(jobId);
  emitSystem(`[${jobId}] Killed (${reason})`);
}

function runCommand(id, cmd) {
  const shell = process.platform === 'win32' ? 'cmd.exe' : 'bash';
  const args = process.platform === 'win32' ? ['/c', cmd] : ['-c', cmd];
  const proc = spawn(shell, args, {
    cwd, env: { ...env }, windowsHide: true, shell: true, detached: false
  });
  const job = { id, proc, startedAt: Date.now(), stdoutBuf: '', stderrBuf: '', killTimer: null };
  jobs.set(id, job);

  const appendLimited = (field, chunk) => {
    const j = jobs.get(id); if (!j) return;
    j[field] += chunk;
    if (j[field].length > 65536) j[field] = j[field].slice(-65536);
  };

  proc.stdout.on('data', (chunk) => {
    const j = jobs.get(id); if (!j) return;
    appendLimited('stdoutBuf', chunk.toString());
    if (j.stdoutBuf.length >= 4096) { emitOutput(`[${id}] ${j.stdoutBuf.trimEnd()}`); j.stdoutBuf = ''; }
  });
  proc.stderr.on('data', (chunk) => {
    const j = jobs.get(id); if (!j) return;
    appendLimited('stderrBuf', chunk.toString());
    if (j.stderrBuf.length >= 4096) { emitError(`[${id}] ${j.stderrBuf.trimEnd()}`); j.stderrBuf = ''; }
  });
  proc.on('error', (err) => {
    const j = jobs.get(id); if (!j) return;
    if (j.killTimer) clearTimeout(j.killTimer);
    emitError(`[${id}] ${err.message}`);
    emitSystem(`[${id}] Failed`);
    jobs.delete(id);
  });
  proc.on('close', (code, signal) => {
    const j = jobs.get(id); if (!j) return;
    if (j.killTimer) clearTimeout(j.killTimer);
    const out = j.stdoutBuf.trim();
    const errOut = j.stderrBuf.trim();
    if (out) emitOutput(`[${id}] ${out}`);
    if (errOut) emitError(`[${id}] ${errOut}`);
    if (signal) emitError(`[${id}] Killed by ${signal}`);
    else emitSystem(`[${id}] Finished with exit code ${code}`);
    jobs.delete(id);
  });
}

function start() {
  const url = process.argv[2];
  if (!url) {
    console.error('[Client] Usage: node script.js <server_url>');
    process.exit(1);
  }
  clientId = loadClientId();
  console.log(`[Client] Loaded client ID: ${clientId}`);
  currentUrl = url;
  console.log(`[Client] Using server URL: ${currentUrl}`);
  connectToServer(currentUrl);
  console.log('[Client] Started. Listening for commands...');
  process.on('exit', () => {
    if (socket) { manuallyDisconnected = true; socket.disconnect(); }
  });
}

process.on('SIGINT', () => {
  console.log('[Client] Received SIGINT');
  manuallyDisconnected = true;
  for (const [jobId] of jobs) killJob(jobId, 'SIGINT');
  if (socket) { socket.disconnect(); socket = null; }
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('[Client] Received SIGTERM');
  manuallyDisconnected = true;
  for (const [jobId] of jobs) killJob(jobId, 'SIGTERM');
  if (socket) { socket.disconnect(); socket = null; }
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  console.error('[Client] Uncaught exception:', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[Client] Unhandled rejection at:', promise, 'reason:', reason);
});

start();