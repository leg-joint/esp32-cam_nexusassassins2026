// ============================================================
// ESP32-CAM cloud relay
// ------------------------------------------------------------
// One ESP32-CAM connects here over a single outbound WebSocket
// and uploads JPEG frames. This server:
//
//   GET /          -> the dashboard (public/index.html)
//   GET /stream    -> MJPEG stream relayed to ANY number of viewers
//   GET /capture   -> a single fresh JPEG frame (Take Photo)
//   GET /flip      -> report flip state  /  GET /flip?v=1&h=0 -> set it
//   GET /status    -> { camera, viewers, frames, v, h }
//   WS  /cam       -> the ESP32's uplink (binary frames up, JSON commands down)
//
// There is exactly ONE camera uplink no matter how many people
// watch. Viewers are served from the server's newest frame, so
// viewer count adds zero load to the ESP32.
// ============================================================

const path = require('path');
const http = require('http');
const { EventEmitter } = require('events');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

// Optional shared key. Set CAM_KEY in Render's environment
// variables and put the same value in the ESP32 sketch's
// RENDER_PATH as "/cam?key=YOURKEY". Empty = no key check.
const CAM_KEY = process.env.CAM_KEY || '';

// Relay ceiling for viewers. The ESP32 sends at its own pace;
// this only caps how often we push frames out to browsers.
const RELAY_FPS = 25;

// If a viewer's connection has more than this much unsent data
// buffered, we skip frames for them instead of letting one slow
// phone build up a backlog and drag its feed further behind live.
const MAX_VIEWER_BACKLOG = 512 * 1024;

// When the last viewer stops, wait this long before telling the
// ESP32 to pause its upload - a viewer reloading the page or the
// dashboard restarting its <img> shouldn't flap the camera.
const STREAM_STOP_GRACE_MS = 8000;

// ------------------------------------------------------------
// State
// ------------------------------------------------------------

let cam = null;              // the ESP32's WebSocket, or null
let latestFrame = null;      // Buffer of the newest JPEG
let frameSeq = 0;            // increments on every new frame
let flipState = { v: 1, h: 0 }; // last known camera orientation
const viewers = new Set();   // open /stream responses
const frameEvents = new EventEmitter();
const pendingAcks = new Map(); // command id -> { resolve, timer }
let nextCmdId = 1;
let stopTimer = null;

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
// /stream - MJPEG relay for viewers. Each viewer just gets an
// <img src="/stream">; frames are fanned out from the single
// camera uplink by the relay loop further below.
// ------------------------------------------------------------

app.get('/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
    'Connection': 'close',
  });

  res.lastSeq = -1;
  viewers.add(res);
  updateStreamDemand();

  req.on('close', () => {
    viewers.delete(res);
    updateStreamDemand();
  });
});

// Fan-out loop: at most RELAY_FPS times per second, hand the
// newest frame to every viewer that hasn't seen it yet. Viewers
// whose connection is backed up simply skip frames - they stay
// live instead of drifting behind, and they can't slow down the
// other viewers.
setInterval(() => {
  if (viewers.size === 0 || !latestFrame) return;

  const header = Buffer.from(
    `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${latestFrame.length}\r\n\r\n`
  );

  for (const res of viewers) {
    if (res.lastSeq === frameSeq) continue;
    if (res.writableLength > MAX_VIEWER_BACKLOG) continue;

    res.lastSeq = frameSeq;
    res.write(header);
    res.write(latestFrame);
    res.write('\r\n');
  }
}, Math.round(1000 / RELAY_FPS));

// ------------------------------------------------------------
// /capture - Take Photo. Waits for the next fresh frame (the
// camera is live, so "next frame" IS a fresh photo) and returns
// it as a JPEG. If nobody is streaming right now we ask the
// ESP32 for a single frame first.
// ------------------------------------------------------------

app.get('/capture', async (req, res) => {
  if (!cam) {
    return res.status(503).send('Camera is not connected to the relay.');
  }

  const startSeq = frameSeq;

  if (viewers.size === 0) {
    sendToCam({ cmd: 'snap' });
  }

  try {
    const frame = await waitForNewFrame(startSeq, 5000);
    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Content-Disposition', 'inline; filename=capture.jpg');
    res.send(frame);
  } catch (err) {
    res.status(504).send('Timed out waiting for a frame from the camera.');
  }
});

function waitForNewFrame(sinceSeq, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (frameSeq > sinceSeq && latestFrame) return resolve(latestFrame);

    const timer = setTimeout(() => {
      frameEvents.removeListener('frame', onFrame);
      reject(new Error('timeout'));
    }, timeoutMs);

    function onFrame() {
      if (frameSeq > sinceSeq && latestFrame) {
        clearTimeout(timer);
        frameEvents.removeListener('frame', onFrame);
        resolve(latestFrame);
      }
    }

    frameEvents.on('frame', onFrame);
  });
}

// ------------------------------------------------------------
// /flip - no query params: report last known state.
//         ?v=0|1 and/or ?h=0|1: relay the command down to the
//         ESP32 over its WebSocket and wait for the ack.
// ------------------------------------------------------------

app.get('/flip', async (req, res) => {
  const hasParam = ('v' in req.query) || ('h' in req.query);

  if (!hasParam) {
    return res.json({ ...flipState, camera: !!cam });
  }

  const v = ('v' in req.query) ? (req.query.v === '1' ? 1 : 0) : flipState.v;
  const h = ('h' in req.query) ? (req.query.h === '1' ? 1 : 0) : flipState.h;

  try {
    const ack = await camCommand({ cmd: 'flip', v, h });
    flipState = { v: ack.v, h: ack.h };
    res.json(flipState);
  } catch (err) {
    res.status(503).json({ ok: false, error: err.message });
  }
});

// ------------------------------------------------------------
// /status - polled by the dashboard so it can show whether the
// ESP32 itself is connected (the browser only ever talks to
// this server, so it can't tell otherwise) and how many viewers
// are watching.
// ------------------------------------------------------------

app.get('/status', (req, res) => {
  res.json({
    camera: !!cam,
    viewers: viewers.size,
    frames: frameSeq,
    v: flipState.v,
    h: flipState.h,
  });
});

// ------------------------------------------------------------
// Camera command helper - sends a JSON command down the ESP32's
// WebSocket and resolves with its {"ack":...} reply.
// ------------------------------------------------------------

function camCommand(payload, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    if (!cam || cam.readyState !== WebSocket.OPEN) {
      return reject(new Error('Camera is not connected to the relay.'));
    }

    const id = nextCmdId++;
    const timer = setTimeout(() => {
      pendingAcks.delete(id);
      reject(new Error('Camera did not answer in time.'));
    }, timeoutMs);

    pendingAcks.set(id, { resolve, timer });
    cam.send(JSON.stringify({ ...payload, id }));
  });
}

function sendToCam(obj) {
  if (cam && cam.readyState === WebSocket.OPEN) {
    cam.send(JSON.stringify(obj));
  }
}

// Tell the ESP32 whether anyone is watching. It only uploads
// frames while at least one viewer is connected, which keeps
// its data usage near zero when nobody is looking.
function updateStreamDemand() {
  if (viewers.size > 0) {
    if (stopTimer) {
      clearTimeout(stopTimer);
      stopTimer = null;
    }
    sendToCam({ cmd: 'stream', on: 1 });
  } else if (!stopTimer) {
    stopTimer = setTimeout(() => {
      stopTimer = null;
      if (viewers.size === 0) sendToCam({ cmd: 'stream', on: 0 });
    }, STREAM_STOP_GRACE_MS);
  }
}

// ------------------------------------------------------------
// WebSocket uplink from the ESP32
// ------------------------------------------------------------

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let pathname, key;
  try {
    const url = new URL(req.url, 'http://localhost');
    pathname = url.pathname;
    key = url.searchParams.get('key') || '';
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
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  // Only one camera: a freshly reconnected ESP32 replaces any
  // stale connection that's still lingering.
  if (cam && cam.readyState === WebSocket.OPEN) {
    cam.terminate();
  }

  cam = ws;
  ws.isAlive = true;
  console.log('Camera connected.');

  // If viewers were already waiting for the feed (they opened
  // the dashboard before the camera came online), start the
  // upload immediately.
  updateStreamDemand();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // New camera frame
      latestFrame = Buffer.from(data);
      frameSeq++;
      frameEvents.emit('frame');
      return;
    }

    // Text message: hello (state report) or command ack
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (typeof msg.v === 'number') flipState.v = msg.v;
    if (typeof msg.h === 'number') flipState.h = msg.h;

    if (msg.ack && pendingAcks.has(msg.ack)) {
      const p = pendingAcks.get(msg.ack);
      pendingAcks.delete(msg.ack);
      clearTimeout(p.timer);
      p.resolve(msg);
    }
  });

  ws.on('close', () => {
    if (cam === ws) cam = null;
    console.log('Camera disconnected.');
  });

  ws.on('error', () => {
    if (cam === ws) cam = null;
  });
});

// Detect and drop dead camera connections (e.g. the ESP32 lost
// power without a clean close) so a reconnect can take over.
setInterval(() => {
  if (!cam) return;
  if (!cam.isAlive) {
    cam.terminate();
    cam = null;
    return;
  }
  cam.isAlive = false;
  cam.ping();
}, 20000);

// ------------------------------------------------------------

server.listen(PORT, () => {
  console.log(`Relay listening on port ${PORT}`);
  console.log(`Dashboard:  http://localhost:${PORT}/`);
  console.log(`Camera uplink: ws://localhost:${PORT}/cam`);
});
