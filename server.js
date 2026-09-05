// ============================================================
// ESP32-CAM cloud relay — DUAL CAMERA VERSION
// ------------------------------------------------------------
// Two ESP32-CAMs connect here over separate outbound WebSockets
// (one per camera) and upload JPEG frames. This server:
//
//   GET /                -> the dashboard (public/index.html)
//   GET /stream?id=...   -> MJPEG stream for the requested camera
//   GET /capture?id=...  -> single fresh JPEG frame
//   GET /flip?id=...     -> report / set flip state
//   GET /status?id=...   -> { camera, viewers, frames, v, h }
//   WS  /cam?id=...      -> per-camera ESP32 uplink
//
// Each camera is completely independent: its own connection,
// frame buffer, viewers, flip state, and controls.
// ============================================================

const path = require('path');
const http = require('http');
const { EventEmitter } = require('events');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

// Optional shared key. Set CAM_KEY in Render's environment
// variables and put the same value in each ESP32 sketch's
// RENDER_PATH as "/cam?id=camera1&key=YOURKEY".
const CAM_KEY = process.env.CAM_KEY || '';

// Relay ceiling for viewers per camera.
const RELAY_FPS = 25;

// If a viewer's connection has more than this much unsent data
// buffered, we skip frames for them.
const MAX_VIEWER_BACKLOG = 512 * 1024;

// Grace period before telling a camera to pause its upload.
const STREAM_STOP_GRACE_MS = 8000;

// ------------------------------------------------------------
// Per-camera state
// ------------------------------------------------------------

const cameras = new Map();   // cameraId -> cameraState

function getOrCreateCamera(id) {
  if (!cameras.has(id)) {
    cameras.set(id, {
      ws: null,                    // the ESP32's WebSocket
      latestFrame: null,           // newest JPEG Buffer
      frameSeq: 0,                 // increments on every new frame
      flipState: { v: 1, h: 0 },   // last known orientation
      viewers: new Set(),          // open /stream responses
      frameEvents: new EventEmitter(),
      pendingAcks: new Map(),      // command id -> { resolve, timer }
      nextCmdId: 1,
      stopTimer: null,
    });
  }
  return cameras.get(id);
}

// ------------------------------------------------------------
// Express app
// ------------------------------------------------------------

const app = express();

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

// ------------------------------------------------------------
// /stream — MJPEG relay per camera
// ------------------------------------------------------------

app.get('/stream', (req, res) => {
  const camId = req.query.id || 'camera1';
  const camState = getOrCreateCamera(camId);

  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
    'Connection': 'close',
  });

  res.lastSeq = -1;
  camState.viewers.add(res);
  updateStreamDemand(camId);

  req.on('close', () => {
    camState.viewers.delete(res);
    updateStreamDemand(camId);
  });
});

// Fan-out loop: at most RELAY_FPS times per second, hand the
// newest frame to every viewer that hasn't seen it yet.
setInterval(() => {
  for (const [camId, camState] of cameras) {
    if (camState.viewers.size === 0 || !camState.latestFrame) continue;

    const header = Buffer.from(
      `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${camState.latestFrame.length}\r\n\r\n`
    );

    for (const res of camState.viewers) {
      if (res.lastSeq === camState.frameSeq) continue;
      if (res.writableLength > MAX_VIEWER_BACKLOG) continue;

      res.lastSeq = camState.frameSeq;
      res.write(header);
      res.write(camState.latestFrame);
      res.write('\r\n');
    }
  }
}, Math.round(1000 / RELAY_FPS));

// ------------------------------------------------------------
// /capture — Take Photo per camera
// ------------------------------------------------------------

app.get('/capture', async (req, res) => {
  const camId = req.query.id || 'camera1';
  const camState = getOrCreateCamera(camId);

  if (!camState.ws) {
    return res.status(503).send('Camera is not connected to the relay.');
  }

  const startSeq = camState.frameSeq;

  if (camState.viewers.size === 0) {
    sendToCam(camId, { cmd: 'snap' });
  }

  try {
    const frame = await waitForNewFrame(camState, startSeq, 5000);
    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Content-Disposition', 'inline; filename=capture.jpg');
    res.send(frame);
  } catch (err) {
    res.status(504).send('Timed out waiting for a frame from the camera.');
  }
});

function waitForNewFrame(camState, sinceSeq, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (camState.frameSeq > sinceSeq && camState.latestFrame) {
      return resolve(camState.latestFrame);
    }

    const timer = setTimeout(() => {
      camState.frameEvents.removeListener('frame', onFrame);
      reject(new Error('timeout'));
    }, timeoutMs);

    function onFrame() {
      if (camState.frameSeq > sinceSeq && camState.latestFrame) {
        clearTimeout(timer);
        camState.frameEvents.removeListener('frame', onFrame);
        resolve(camState.latestFrame);
      }
    }

    camState.frameEvents.on('frame', onFrame);
  });
}

// ------------------------------------------------------------
// /flip — per camera
// ------------------------------------------------------------

app.get('/flip', async (req, res) => {
  const camId = req.query.id || 'camera1';
  const camState = getOrCreateCamera(camId);
  const hasParam = ('v' in req.query) || ('h' in req.query);

  if (!hasParam) {
    return res.json({ ...camState.flipState, camera: !!camState.ws });
  }

  const v = ('v' in req.query) ? (req.query.v === '1' ? 1 : 0) : camState.flipState.v;
  const h = ('h' in req.query) ? (req.query.h === '1' ? 1 : 0) : camState.flipState.h;

  try {
    const ack = await camCommand(camId, { cmd: 'flip', v, h });
    camState.flipState = { v: ack.v, h: ack.h };
    res.json(camState.flipState);
  } catch (err) {
    res.status(503).json({ ok: false, error: err.message });
  }
});

// ------------------------------------------------------------
// /status — per camera
// ------------------------------------------------------------

app.get('/status', (req, res) => {
  const camId = req.query.id || 'camera1';
  const camState = getOrCreateCamera(camId);
  res.json({
    camera: !!camState.ws,
    viewers: camState.viewers.size,
    frames: camState.frameSeq,
    v: camState.flipState.v,
    h: camState.flipState.h,
  });
});

// ------------------------------------------------------------
// Camera command helpers
// ------------------------------------------------------------

function camCommand(camId, payload, timeoutMs = 3000) {
  const camState = getOrCreateCamera(camId);
  return new Promise((resolve, reject) => {
    if (!camState.ws || camState.ws.readyState !== WebSocket.OPEN) {
      return reject(new Error('Camera is not connected to the relay.'));
    }

    const id = camState.nextCmdId++;
    const timer = setTimeout(() => {
      camState.pendingAcks.delete(id);
      reject(new Error('Camera did not answer in time.'));
    }, timeoutMs);

    camState.pendingAcks.set(id, { resolve, timer });
    camState.ws.send(JSON.stringify({ ...payload, id }));
  });
}

function sendToCam(camId, obj) {
  const camState = getOrCreateCamera(camId);
  if (camState.ws && camState.ws.readyState === WebSocket.OPEN) {
    camState.ws.send(JSON.stringify(obj));
  }
}

function updateStreamDemand(camId) {
  const camState = getOrCreateCamera(camId);
  if (camState.viewers.size > 0) {
    if (camState.stopTimer) {
      clearTimeout(camState.stopTimer);
      camState.stopTimer = null;
    }
    sendToCam(camId, { cmd: 'stream', on: 1 });
  } else if (!camState.stopTimer) {
    camState.stopTimer = setTimeout(() => {
      camState.stopTimer = null;
      if (camState.viewers.size === 0) sendToCam(camId, { cmd: 'stream', on: 0 });
    }, STREAM_STOP_GRACE_MS);
  }
}

// ------------------------------------------------------------
// WebSocket uplink from each ESP32
// ------------------------------------------------------------

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let pathname, key, camId;
  try {
    const url = new URL(req.url, 'http://localhost');
    pathname = url.pathname;
    key = url.searchParams.get('key') || '';
    camId = url.searchParams.get('id') || 'camera1';
  } catch {
    socket.destroy();
    return;
  }

  if (pathname !== '/cam') {
    socket.destroy();
    return;
  }

  if (CAM_KEY && key !== CAM_KEY) {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.camId = camId;
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  const camId = ws.camId || 'camera1';
  const camState = getOrCreateCamera(camId);

  // A freshly reconnected ESP32 replaces any stale connection
  // for the SAME camera id. Different ids coexist.
  if (camState.ws && camState.ws.readyState === WebSocket.OPEN) {
    camState.ws.terminate();
  }

  camState.ws = ws;
  ws.isAlive = true;
  console.log(`Camera [${camId}] connected.`);

  updateStreamDemand(camId);

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    const state = getOrCreateCamera(ws.camId || 'camera1');

    if (isBinary) {
      state.latestFrame = Buffer.from(data);
      state.frameSeq++;
      state.frameEvents.emit('frame');
      return;
    }

    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (typeof msg.v === 'number') state.flipState.v = msg.v;
    if (typeof msg.h === 'number') state.flipState.h = msg.h;

    if (msg.ack && state.pendingAcks.has(msg.ack)) {
      const p = state.pendingAcks.get(msg.ack);
      state.pendingAcks.delete(msg.ack);
      clearTimeout(p.timer);
      p.resolve(msg);
    }
  });

  ws.on('close', () => {
    const state = getOrCreateCamera(ws.camId || 'camera1');
    if (state.ws === ws) {
      state.ws = null;
      console.log(`Camera [${ws.camId || 'camera1'}] disconnected.`);
    }
  });

  ws.on('error', () => {
    const state = getOrCreateCamera(ws.camId || 'camera1');
    if (state.ws === ws) state.ws = null;
  });
});

// Detect and drop dead camera connections per camera.
setInterval(() => {
  for (const [camId, camState] of cameras) {
    if (!camState.ws) continue;
    if (!camState.ws.isAlive) {
      camState.ws.terminate();
      camState.ws = null;
      console.log(`Camera [${camId}] timed out (no pong).`);
      continue;
    }
    camState.ws.isAlive = false;
    camState.ws.ping();
  }
}, 20000);

// ------------------------------------------------------------

server.listen(PORT, () => {
  console.log(`Relay listening on port ${PORT}`);
  console.log(`Dashboard:  http://localhost:${PORT}/`);
  console.log(`Camera uplink: ws://localhost:${PORT}/cam?id=camera1`);
  console.log(`Camera uplink: ws://localhost:${PORT}/cam?id=camera2`);
});
