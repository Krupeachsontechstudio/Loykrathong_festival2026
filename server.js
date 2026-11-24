// server.js — เซิร์ฟเวอร์เกม "ลอยกระทงออนไลน์": หน้าเว็บ + WebSocket เรียลไทม์ + REST API (OpenAPI)
//
// ติดตั้ง:  npm install      รัน:  npm start      แล้วเปิด http://localhost:3000
// เอกสาร API:  http://localhost:3000/docs     สเปก OpenAPI:  /openapi.json
//
// ตัวแปรแวดล้อม
//   PORT         พอร์ต (บริการ hosting ตั้งให้เอง)
//   API_KEY      รหัสสำหรับ API ที่สั่งเกม (POST) — ถ้าไม่ตั้ง POST ทั้งหมดจะถูกปิด
//   ADMIN_PASSWORD  รหัสแอดมินในเกม (ใช้สั่งเอฟเฟกต์ทั้งงานจากแผงแอดมิน)
//   MAX_CLIENTS  จำนวนผู้เล่นพร้อมกันสูงสุด (ค่าเริ่มต้น 300)
//   DATA_FILE    ไฟล์เก็บยอดรวม (ค่าเริ่มต้น data.json ข้างไฟล์นี้)

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
const MAX_CLIENTS = +process.env.MAX_CLIENTS || 300;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const MAX_BYTES = 4096, MAX_MSG_PER_SEC = 30;
const started = Date.now();

/* ---------- ยอดรวมและคำอธิษฐาน (เก็บลงไฟล์) ---------- */
let data = { krathong: 0, lantern: 0, pray: 0, dress: 0, wishes: [] };
try { data = Object.assign(data, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); } catch (e) {}
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(DATA_FILE, JSON.stringify(data), () => {}); } }, 5000);

const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/https?:\/\/\S+|www\.\S+/gi, '').trim().slice(0, n);
const num = (v, a, b, d) => { v = +v; return Number.isFinite(v) ? Math.max(a, Math.min(b, v)) : d; };

/* ---------- Server-Sent Events สำหรับแอปอื่น ---------- */
const sse = new Set();
function emit(event, payload) {
  const s = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const res of sse) res.write(s);
}
setInterval(() => { for (const res of sse) res.write(': ping\n\n'); }, 25000);

/* ---------- หน้าเว็บ ---------- */
const page = () => fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8')
  .replace('<head>', '<head>\n<script>window.LK_SERVER=location.origin.replace(/^http/,"ws")</script>');

const DOCS = `<!DOCTYPE html><html lang="th"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Loy Krathong API</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"></head>
<body><div id="ui"></div><script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#ui'})</script></body></html>`;

/* ---------- สเปก OpenAPI 3.0 ---------- */
const playerSchema = { type: 'object', properties: {
  id: { type: 'string' }, name: { type: 'string' }, x: { type: 'number' }, z: { type: 'number' },
  outfit: { type: 'string', description: 'ชุดไทย เช่น f1–f5 (หญิง) m1–m5 (ชาย) หรือว่าง', example: 'f3' },
  holding: { type: 'string', enum: ['', 'k', 'l'], description: 'k = ถือกระทง, l = ถือโคม' },
  krathong: { type: 'integer' }, lantern: { type: 'integer' } } };
const wishSchema = { type: 'object', properties: {
  name: { type: 'string' }, action: { type: 'string', enum: ['float', 'lantern', 'pray'] }, wish: { type: 'string' }, at: { type: 'string', format: 'date-time' } } };
const okSchema = { type: 'object', properties: { ok: { type: 'boolean' }, delivered: { type: 'integer', description: 'จำนวนหน้าเกมที่ได้รับคำสั่ง' } } };
const keyErr = { '401': { description: 'API key ไม่ถูกต้อง' }, '403': { description: 'เซิร์ฟเวอร์ยังไม่ได้ตั้ง API_KEY' }, '429': { description: 'เรียกถี่เกินไป' } };
const OPENAPI = {
  openapi: '3.0.3',
  info: { title: 'Loy Krathong Online API', version: '1.0.0',
    description: 'API ของเกมลอยกระทงออนไลน์ อ่านสถานะงาน ผู้เล่นออนไลน์ คำอธิษฐาน ฟังเหตุการณ์แบบเรียลไทม์ และส่งคำสั่งเข้าเกม (ต้องใช้ API key)' },
  servers: [{ url: '/' }],
  components: { securitySchemes: { apiKey: { type: 'apiKey', in: 'header', name: 'X-API-Key' } } },
  paths: {
    '/api/status': { get: { summary: 'สถานะงานและยอดรวม', tags: ['อ่านข้อมูล'], responses: { '200': { description: 'OK', content: { 'application/json': { schema: { type: 'object', properties: {
      online: { type: 'integer' }, krathong: { type: 'integer' }, lantern: { type: 'integer' }, pray: { type: 'integer' }, dress: { type: 'integer' }, uptimeSec: { type: 'integer' } } } } } } } } },
    '/api/players': { get: { summary: 'รายชื่อผู้เล่นที่ออนไลน์ตอนนี้', tags: ['อ่านข้อมูล'], responses: { '200': { description: 'OK', content: { 'application/json': { schema: { type: 'array', items: playerSchema } } } } } } },
    '/api/wishes': { get: { summary: 'คำอธิษฐานล่าสุด', tags: ['อ่านข้อมูล'], parameters: [{ name: 'limit', in: 'query', schema: { type: 'integer', minimum: 1, maximum: 100, default: 20 } }],
      responses: { '200': { description: 'OK', content: { 'application/json': { schema: { type: 'array', items: wishSchema } } } } } } },
    '/api/events': { get: { summary: 'ฟังเหตุการณ์แบบเรียลไทม์ (Server-Sent Events)', tags: ['เรียลไทม์'],
      description: 'เหตุการณ์: join, leave, float, lantern, pray, dress, photo, firework, chat, command, gift, fx — ข้อมูลเป็น JSON ในบรรทัด data:',
      responses: { '200': { description: 'สตรีม text/event-stream', content: { 'text/event-stream': { schema: { type: 'string' } } } } } } },
    '/api/command': { post: { summary: 'ส่งคำสั่งเหมือนคอมเมนต์ในแชต', tags: ['สั่งเกม'], security: [{ apiKey: [] }],
      description: 'ข้อความใช้รูปแบบเดียวกับแชต เช่น "ลอยกระทง ขอให้สอบผ่าน", "ปล่อยโคม", "เที่ยวงาน", "อธิษฐาน ..." — ทุกหน้าเกมจะมีตัวละครชื่อนี้เดินมาทำกิจกรรม',
      requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', required: ['user', 'text'], properties: {
        user: { type: 'string', maxLength: 18, example: 'น้องดาว' }, text: { type: 'string', maxLength: 80, example: 'ลอยกระทง ขอให้สอบผ่าน' } } } } } },
      responses: Object.assign({ '200': { description: 'ส่งแล้ว', content: { 'application/json': { schema: okSchema } } } }, keyErr) } },
    '/api/gift': { post: { summary: 'ส่งของขวัญ (เอฟเฟกต์ตามที่ตั้งในเกม)', tags: ['สั่งเกม'], security: [{ apiKey: [] }],
      requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', required: ['user', 'gift'], properties: {
        user: { type: 'string', example: 'บอส' }, gift: { type: 'string', example: 'Rose' }, count: { type: 'integer', minimum: 1, maximum: 99, default: 1 } } } } } },
      responses: Object.assign({ '200': { description: 'ส่งแล้ว', content: { 'application/json': { schema: okSchema } } } }, keyErr) } },
    '/api/fx': { post: { summary: 'เล่นเอฟเฟกต์ทั้งงาน', tags: ['สั่งเกม'], security: [{ apiKey: [] }],
      requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', required: ['fx'], properties: {
        fx: { type: 'string', enum: ['fireworks', 'petals', 'goldrain', 'lanterns'] } } } } } },
      responses: Object.assign({ '200': { description: 'ส่งแล้ว', content: { 'application/json': { schema: okSchema } } } }, keyErr) } },
  },
};

/* ---------- ช่วยตอบ HTTP ---------- */
function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
function readJson(req) {
  return new Promise((ok, bad) => {
    let b = ''; req.on('data', c => { b += c; if (b.length > 2048) { bad(new Error('ข้อมูลใหญ่เกินไป')); req.destroy(); } });
    req.on('end', () => { try { ok(JSON.parse(b || '{}')); } catch (e) { bad(new Error('JSON ไม่ถูกต้อง')); } });
  });
}
const postHits = new Map();
function guard(req, res) {
  const adminOk = ADMIN_PASSWORD && req.headers['x-admin-password'] === ADMIN_PASSWORD;
  if (!adminOk) {
    if (!API_KEY) { send(res, 403, { error: 'ยังไม่ได้ตั้ง API_KEY บนเซิร์ฟเวอร์' }); return false; }
    if (req.headers['x-api-key'] !== API_KEY) { send(res, 401, { error: 'API key ไม่ถูกต้อง' }); return false; }
  }
  const ip = req.socket.remoteAddress || '', now = Date.now(), h = postHits.get(ip) || [];
  const recent = h.filter(t => now - t < 10000); recent.push(now); postHits.set(ip, recent);
  if (recent.length > 20) { send(res, 429, { error: 'เรียกถี่เกินไป (สูงสุด 20 ครั้งต่อ 10 วินาที)' }); return false; }
  return true;
}
const players = () => [...peers.values()].filter(p => p.presence.nm).map(p => ({
  id: p.id, name: clean(p.presence.nm, 18), x: num(p.presence.x, -45, 45, 0), z: num(p.presence.z, -40, 18, 0),
  outfit: /^[fm][1-5]$/.test(p.presence.o || '') ? p.presence.o : '', holding: ['k', 'l'].includes(p.presence.k) ? p.presence.k : '',
  krathong: num(p.presence.n, 0, 9999, 0) | 0, lantern: num(p.presence.l, 0, 9999, 0) | 0 }));
function toGames(obj) { const s = JSON.stringify(obj); let n = 0; for (const ws of wss.clients) if (ws.readyState === 1) { ws.send(s); n++; } return n; }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x'), p = url.pathname;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type,X-API-Key' });
    return res.end();
  }
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return res.end(page());
    }
    if (req.method === 'GET' && p === '/health') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'GET' && p === '/openapi.json') return send(res, 200, OPENAPI);
    if (req.method === 'GET' && p === '/docs') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(DOCS); }
    if (req.method === 'GET' && p === '/api/status') {
      return send(res, 200, { online: players().length, krathong: data.krathong, lantern: data.lantern, pray: data.pray, dress: data.dress, uptimeSec: Math.round((Date.now() - started) / 1000) });
    }
    if (req.method === 'GET' && p === '/api/players') return send(res, 200, players());
    if (req.method === 'GET' && p === '/api/wishes') {
      const lim = num(url.searchParams.get('limit'), 1, 100, 20) | 0; return send(res, 200, data.wishes.slice(0, lim));
    }
    if (req.method === 'GET' && p === '/api/events') {
      if (sse.size >= 200) return send(res, 503, { error: 'ผู้ฟังเต็ม' });
      res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' });
      res.write(`event: hello\ndata: ${JSON.stringify({ online: players().length, krathong: data.krathong })}\n\n`);
      sse.add(res); req.on('close', () => sse.delete(res)); return;
    }
    if (req.method === 'POST' && p === '/api/command') {
      if (!guard(req, res)) return; const b = await readJson(req);
      const user = clean(b.user, 18), text = clean(b.text, 80);
      if (!user || !text) return send(res, 400, { error: 'ต้องมี user และ text' });
      emit('command', { user, text }); return send(res, 200, { ok: true, delivered: toGames({ t: 'cmd', user, text }) });
    }
    if (req.method === 'POST' && p === '/api/gift') {
      if (!guard(req, res)) return; const b = await readJson(req);
      const user = clean(b.user, 18), gift = clean(b.gift, 24), count = num(b.count, 1, 99, 1) | 0;
      if (!user || !gift) return send(res, 400, { error: 'ต้องมี user และ gift' });
      emit('gift', { user, gift, count }); return send(res, 200, { ok: true, delivered: toGames({ t: 'gift', user, gift, count }) });
    }
    if (req.method === 'POST' && p === '/api/fx') {
      if (!guard(req, res)) return; const b = await readJson(req);
      if (!['fireworks', 'petals', 'goldrain', 'lanterns'].includes(b.fx)) return send(res, 400, { error: 'fx ต้องเป็น fireworks, petals, goldrain หรือ lanterns' });
      emit('fx', { fx: b.fx }); return send(res, 200, { ok: true, delivered: toGames({ t: 'fx', fx: b.fx }) });
    }
    send(res, 404, { error: 'ไม่พบ' });
  } catch (e) { send(res, 400, { error: e.message }); }
});

/* ---------- WebSocket: ผู้เล่นเรียลไทม์ ---------- */
const wss = new WebSocketServer({ server, maxPayload: MAX_BYTES });
const peers = new Map();
let nextId = 1;
function broadcast(obj, except) { const s = JSON.stringify(obj); for (const ws of wss.clients) if (ws !== except && ws.readyState === 1) ws.send(s); }
function pushTotals() { broadcast({ t: 'totals', k: data.krathong, l: data.lantern }); }

function track(me, pres) {
  const name = clean(pres.nm, 18);
  if (name && !me.joined) { me.joined = true; emit('join', { id: me.id, name }); }
  const o = /^[fm][1-5]$/.test(pres.o || '') ? pres.o : '';
  if (me.joined && o && o !== me.outfit) { data.dress++; dirty = true; emit('dress', { id: me.id, name, outfit: o }); }
  me.outfit = o;
  const w = pres.w;
  if (w && typeof w === 'object' && w.i !== me.lastAct && ['float', 'lantern', 'pray'].includes(w.a)) {
    me.lastAct = w.i;
    const item = { name, action: w.a, wish: clean(w.t, 60), at: new Date().toISOString() };
    if (w.a === 'float') data.krathong++; else if (w.a === 'lantern') data.lantern++; else data.pray++;
    data.wishes.unshift(item); data.wishes.length = Math.min(data.wishes.length, 100); dirty = true;
    emit(w.a, item); if (w.a !== 'pray') pushTotals();
  }
  if (w && typeof w === 'object' && w.a === 'photo' && w.i !== me.lastPhoto) { me.lastPhoto = w.i; emit('photo', { name }); }
  if (w && typeof w === 'object' && w.a === 'fw' && w.i !== me.lastFw) { me.lastFw = w.i; const t = ['Love you', 'Stay healthy', 'Good luck'][(num(w.kt, 1, 3, 1) | 0) - 1]; emit('firework', { name, text: t }); }
  const m = pres.m;
  if (m && typeof m === 'object' && m.i !== me.lastMsg) { me.lastMsg = m.i; const t = clean(m.t, 60); if (t) emit('chat', { name, text: t, to: clean(m.to, 18) }); }
}

wss.on('connection', ws => {
  if (wss.clients.size > MAX_CLIENTS) { ws.close(1013, 'เต็ม'); return; }
  const me = { id: (nextId++).toString(36) + Math.random().toString(36).slice(2, 6), presence: {}, count: 0, windowStart: Date.now(), alive: true, joined: false, lastAct: undefined, outfit: '' };
  peers.set(ws, me);
  ws.send(JSON.stringify({ t: 'hello', id: me.id, peers: [...peers.values()].filter(p => p !== me && p.presence.nm).map(p => ({ id: p.id, presence: p.presence })) }));
  ws.send(JSON.stringify({ t: 'totals', k: data.krathong, l: data.lantern }));
  ws.on('pong', () => { me.alive = true; });
  ws.on('message', buf => {
    const now = Date.now();
    if (now - me.windowStart > 1000) { me.windowStart = now; me.count = 0; }
    if (++me.count > MAX_MSG_PER_SEC) return;
    let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!m || m.t !== 'p' || !m.presence || typeof m.presence !== 'object' || Array.isArray(m.presence)) return;
    if (!me.seen) { me.seen = true; me.lastAct = m.presence.w && m.presence.w.i; me.outfit = /^[fm][1-5]$/.test(m.presence.o || '') ? m.presence.o : ''; me.lastMsg = m.presence.m && m.presence.m.i; me.lastPhoto = m.presence.w && m.presence.w.i; me.lastFw = me.lastPhoto; } // ไม่นับซ้ำเมื่อต่อใหม่
    me.presence = m.presence; track(me, m.presence);
    broadcast({ t: 'p', id: me.id, presence: me.presence }, ws);
  });
  ws.on('close', () => { peers.delete(ws); broadcast({ t: 'leave', id: me.id }); if (me.joined) emit('leave', { id: me.id, name: clean(me.presence.nm, 18) }); });
  ws.on('error', () => {});
});
setInterval(() => {
  for (const ws of wss.clients) { const p = peers.get(ws); if (!p) continue; if (!p.alive) { ws.terminate(); continue; } p.alive = false; try { ws.ping(); } catch (e) {} }
}, 20000);

server.listen(PORT, () => {
  console.log(`งานลอยกระทงเปิดแล้วที่ http://localhost:${PORT}`);
  console.log(`เอกสาร API: http://localhost:${PORT}/docs` + (API_KEY ? '' : '   (ยังไม่ได้ตั้ง API_KEY — คำสั่ง POST ถูกปิดไว้)'));
});
