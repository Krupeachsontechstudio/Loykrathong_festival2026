// connector.js — ตัวกลางส่งคอมเมนต์/ของขวัญจาก LIVE จริง เข้าเกม "ลอยกระทงออนไลน์"
// รันบนเครื่องเดียวกับ OBS:  node connector.js
// ต้องมี Node.js 18 ขึ้นไป และติดตั้งก่อน:  npm i ws tiktok-live-connector
//
// ตั้งค่าผ่านตัวแปรแวดล้อม (Windows PowerShell ตัวอย่าง):
//   $env:TIKTOK_USER="ชื่อบัญชีtiktokไม่มี@"
//   $env:FB_LIVE_ID="ไอดีวิดีโอไลฟ์ของเพจ"      (ไม่ใส่ = ไม่ต่อ Facebook)
//   $env:FB_TOKEN="Page Access Token"
//   node connector.js
//
// หมายเหตุสำคัญ
// - TikTok: ใช้ไลบรารี tiktok-live-connector ซึ่ง "ไม่ใช่ API ทางการ" ของ TikTok อาจหยุดทำงานได้เมื่อ TikTok เปลี่ยนระบบ
//   และต้องกำลังไลฟ์อยู่จริงจึงจะเชื่อมต่อได้
// - Facebook: ใช้ได้เฉพาะไลฟ์ของ "เพจ" ที่คุณเป็นแอดมิน และต้องมี Page Access Token ที่มีสิทธิ์อ่านความคิดเห็น
//   ชื่อผู้คอมเมนต์อาจไม่ถูกส่งมาสำหรับผู้ใช้ทั่วไป (ข้อจำกัดความเป็นส่วนตัวของ Meta) เกมจะแสดงเป็น "ผู้ชม"
//   ไลฟ์จากโปรไฟล์ส่วนตัวอ่านคอมเมนต์ไม่ได้ แต่ยังถ่ายทอดภาพเกมผ่าน OBS ได้ตามปกติ

const { WebSocketServer } = require('ws');
const PORT = +process.env.PORT || 8787;
const wss = new WebSocketServer({ port: PORT });
const status = {};
function send(msg) { const s = JSON.stringify(msg); for (const c of wss.clients) if (c.readyState === 1) c.send(s); }
function setStatus(platform, state, error) { status[platform] = { state, error }; send({ type: 'status', platform, state, error }); console.log(`[${platform}] ${state}${error ? ' — ' + error : ''}`); }
wss.on('connection', ws => { for (const [platform, s] of Object.entries(status)) ws.send(JSON.stringify({ type: 'status', platform, ...s })); });
console.log(`ตัวกลางพร้อมที่ ws://localhost:${PORT}  (ใส่ที่อยู่นี้ในหน้า ตั้งค่า > การเชื่อมต่อ LIVE ของเกม)`);

/* ---------------- TikTok LIVE ---------------- */
const TT = process.env.TIKTOK_USER;
if (TT) {
  let mod;
  try { mod = require('tiktok-live-connector'); } catch (e) { console.log('ยังไม่ได้ติดตั้ง tiktok-live-connector: npm i tiktok-live-connector'); }
  const Conn = mod && (mod.TikTokLiveConnection || mod.WebcastPushConnection);
  if (Conn) {
    const tt = new Conn(TT);
    const who = d => { const u = d.user || d; return u.nickname || u.uniqueId || 'ผู้ชม'; };
    tt.on('chat', d => send({ type: 'chat', platform: 'tiktok', user: who(d), text: d.comment || '' }));
    tt.on('gift', d => {
      if (d.giftType === 1 && !d.repeatEnd) return; // ของขวัญแบบกดรัว: รอจนจบชุดแล้วส่งครั้งเดียว
      const name = d.giftName || (d.giftDetails && d.giftDetails.giftName) || 'ของขวัญ';
      send({ type: 'gift', platform: 'tiktok', user: who(d), gift: name, count: d.repeatCount || 1 });
    });
    tt.on('roomUser', d => { if (Number.isFinite(d.viewerCount)) send({ type: 'viewers', platform: 'tiktok', count: d.viewerCount }); });
    tt.on('streamEnd', () => setStatus('tiktok', 'disconnected', 'ไลฟ์จบแล้ว'));
    tt.on('disconnected', () => { setStatus('tiktok', 'disconnected'); setTimeout(connectTT, 10000); });
    function connectTT() {
      setStatus('tiktok', 'connecting');
      tt.connect().then(() => setStatus('tiktok', 'connected'))
        .catch(e => { setStatus('tiktok', 'disconnected', String(e && e.message || e).slice(0, 160)); setTimeout(connectTT, 15000); });
    }
    connectTT();
  }
}

/* ---------------- Facebook Page LIVE (Live Comments stream) ---------------- */
const FB_ID = process.env.FB_LIVE_ID, FB_TOKEN = process.env.FB_TOKEN;
if (FB_ID && FB_TOKEN) {
  async function connectFB() {
    setStatus('facebook', 'connecting');
    const url = `https://streaming-graph.facebook.com/${FB_ID}/live_comments?access_token=${encodeURIComponent(FB_TOKEN)}&comment_rate=ten_per_second&fields=from{name},message`;
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + (await res.text()).slice(0, 140));
      setStatus('facebook', 'connected');
      const dec = new TextDecoder(); let buf = '';
      for await (const chunk of res.body) {
        buf += dec.decode(chunk, { stream: true });
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1);
          if (!line.startsWith('data:')) continue;
          try { const c = JSON.parse(line.slice(5)); send({ type: 'chat', platform: 'facebook', user: (c.from && c.from.name) || 'ผู้ชม', text: c.message || '' }); } catch (e) {}
        }
      }
      setStatus('facebook', 'disconnected', 'สตรีมคอมเมนต์ปิดลง');
    } catch (e) { setStatus('facebook', 'disconnected', String(e.message || e).slice(0, 160)); }
    setTimeout(connectFB, 15000);
  }
  connectFB();
}

if (!TT && !(FB_ID && FB_TOKEN)) console.log('ยังไม่ได้ตั้ง TIKTOK_USER หรือ FB_LIVE_ID + FB_TOKEN — ตัวกลางจะรอเฉย ๆ');
