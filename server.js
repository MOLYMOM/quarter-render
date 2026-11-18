// 4분의 1 — game server for Render (no dependencies, Node 18+)
// Serves the game page and relays room messages between players (Server-Sent Events + POST).
// The host's browser runs the game rules; this server only passes messages along.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const INDEX = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;
const MAX_PEERS_PER_ROOM = 16;
const MAX_ROOMS = 1000;
const GRACE_MS = 15000; // keep a seat this long after a dropped connection

const rooms = new Map();  // room name -> Map(peerId -> peer)
const tokens = new Map(); // secret token -> peer

const sse = (res, event, data) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
const peersOf = room => [...(rooms.get(room) || new Map()).values()];
function broadcast(room, event, data) { for (const p of peersOf(room)) if (p.res) sse(p.res, event, data); }
function sendPeers(room) { broadcast(room, 'peers', peersOf(room).map(p => ({ peer: p.id, presence: p.presence }))); }
function removePeer(p) {
  const m = rooms.get(p.room); if (!m || !m.has(p.id)) return;
  m.delete(p.id); tokens.delete(p.token);
  if (!m.size) rooms.delete(p.room); else sendPeers(p.room);
}
function readBody(req, limit = 8192) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', c => { n += c.length; if (n > limit) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const end = (res, code, body = '') => { res.writeHead(code, { 'content-type': 'text/plain; charset=utf-8' }); res.end(body); };

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    return res.end(INDEX);
  }
  // keep-alive / health check: the page calls this every 4 minutes while open
  if (url.pathname === '/ping') {
    res.writeHead(200, { 'content-type': 'text/plain', 'cache-control': 'no-store' });
    return res.end('pong');
  }

  // live stream of room events for one player
  if (req.method === 'GET' && url.pathname === '/events') {
    const room = url.searchParams.get('room') || '';
    if (!ROOM_RE.test(room)) return end(res, 400, 'bad room');
    let p = tokens.get(url.searchParams.get('resume') || '');
    if (p && p.room === room) {           // reconnect: keep the same seat
      clearTimeout(p.grace); p.grace = null;
      if (p.res) { const old = p.res; p.res = null; old.end(); }
    } else {
      const m = rooms.get(room) || new Map();
      if (m.size >= MAX_PEERS_PER_ROOM) return end(res, 429, 'room full');
      if (!rooms.has(room) && rooms.size >= MAX_ROOMS) return end(res, 503, 'busy');
      p = { id: crypto.randomBytes(8).toString('hex'), token: crypto.randomBytes(16).toString('hex'), room, presence: {}, res: null, grace: null, rl: [] };
      m.set(p.id, p); rooms.set(room, m); tokens.set(p.token, p);
    }
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    res.write('retry: 2000\n\n');
    p.res = res;
    sse(res, 'hello', { peer: p.id, token: p.token });
    sendPeers(room);
    const hb = setInterval(() => res.write(': hb\n\n'), 20000);
    req.on('close', () => {
      clearInterval(hb);
      if (p.res !== res) return;
      p.res = null;
      p.grace = setTimeout(() => removePeer(p), GRACE_MS);
    });
    return;
  }

  if (req.method === 'POST' && ['/send', '/presence', '/leave'].includes(url.pathname)) {
    let b; try { b = await readBody(req); } catch (e) { return end(res, 413); }
    const p = tokens.get(typeof b.token === 'string' ? b.token : '');
    if (!p || p.room !== b.room) return end(res, 403);
    if (url.pathname === '/send') {
      if (typeof b.topic !== 'string' || !TOPIC_RE.test(b.topic)) return end(res, 400);
      const now = Date.now(); p.rl = p.rl.filter(t => now - t < 10000);
      if (p.rl.length >= 80) return end(res, 429);
      p.rl.push(now);
      broadcast(p.room, 'msg', { topic: b.topic, peer: p.id, data: b.data });
    } else if (url.pathname === '/presence') {
      if (b.presence && typeof b.presence === 'object' && !Array.isArray(b.presence) && JSON.stringify(b.presence).length <= 2048) {
        p.presence = b.presence; sendPeers(p.room);
      }
    } else {
      clearTimeout(p.grace);
      if (p.res) { const r = p.res; p.res = null; r.end(); }
      removePeer(p);
    }
    res.writeHead(204); return res.end();
  }

  end(res, 404, 'not found');
});

server.listen(PORT, () => console.log(`4분의 1 server listening on ${PORT}`));
