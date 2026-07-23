import http from 'k6/http';
import { Trend, Counter } from 'k6/metrics';
import { WebSocket } from 'k6/experimental/websockets';
import { setInterval, setTimeout, clearInterval } from 'k6/timers';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.1/index.js';

// ---- 파라미터 (CLI로 덮어쓰기 가능) ----
const USERS = Number(__ENV.USERS || 50);
const TALKERS = Number(__ENV.TALKERS || USERS);  // 실제 송신하는 VU 수
const PORTS = [8081, 8082, 8083];
const HOST = 'localhost';
const SEND_MS  = Number(__ENV.SEND_MS  || 120000);  // 송신 2분
const DRAIN_MS = Number(__ENV.DRAIN_MS || 60000);   // 무송신 드레인 1분

// ---- 커스텀 지표 ----
const deliveryLatency = new Trend('ws_delivery_latency_ms', true);
const sent = new Counter('app_msgs_sent');
const received = new Counter('app_msgs_received');

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || '30s',
  thresholds: {
    'ws_delivery_latency_ms': ['p(95)<500'],
  },
};

function authHeaders(token) {
  return { headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } };
}
function jsonHeaders() {
  return { headers: { 'Content-Type': 'application/json' } };
}

// =====================================================================
//  Snowflake ID 안전 추출 헬퍼
//  JSON.parse()는 2^53 초과 정수의 정밀도를 깨뜨리므로,
//  원시 JSON 문자열에서 정규식으로 추출한다.
// =====================================================================
function extractFieldAsString(rawJson, fieldName) {
  const regex = new RegExp('"' + fieldName + '"\\s*:\\s*"?(\\d+)"?');
  const match = rawJson.match(regex);
  return match ? match[1] : null;
}

// =====================================================================
//  setup(): 딱 1회 실행. 유저 프로비저닝 + 방 생성 + 전원 입장
// =====================================================================
export function setup() {
  const base = `http://${HOST}:${PORTS[0]}`;
  const users = [];

  // 1) 가입 + 로그인
  for (let i = 1; i <= USERS; i++) {
    const email = `load${i}@test.com`;
    const body = JSON.stringify({ email, password: 'test1234', name: `load${i}` });
    http.post(`${base}/minichat/user/auth/signup`, body, jsonHeaders());

    const loginRes = http.post(`${base}/minichat/user/auth/login`,
      JSON.stringify({ email, password: 'test1234' }), jsonHeaders());
    if (loginRes.status !== 200) { console.error(`login 실패 ${email}: ${loginRes.status}`); continue; }

    const token = loginRes.json().accessToken;
    const userId = extractFieldAsString(loginRes.body, 'userId');
    if (!userId) { console.error(`userId 추출 실패 ${email}`); continue; }

    users.push({ userId, token });
  }
  if (users.length === 0) throw new Error('로그인된 유저가 0명 — 앱/DB 확인');

  // 2) 방 생성 (user[0]이 생성, 전원 멤버로)
  const userIdArrayStr = users.map(u => u.userId).join(',');
  const title = `loadroom-${Date.now()}`;
  const createBody = `{"title":"${title}","userIds":[${userIdArrayStr}]}`;
  const createRes = http.post(`${base}/minichat/chat`, createBody, authHeaders(users[0].token));
  if (createRes.status !== 201) {
    throw new Error(`방 생성 실패: status=${createRes.status} body=${createRes.body}`);
  }

  const chatId = extractFieldAsString(createRes.body, 'id');
  if (!chatId) throw new Error(`chatId 파싱 실패: ${createRes.body}`);
  console.log(`방 생성됨: chatId=${chatId}, 멤버=${users.length}`);

  // 4) 전원 입장
  for (const u of users) {
    const r = http.post(`${base}/minichat/chat/${chatId}/enter`, null, authHeaders(u.token));
    if (r.status !== 201) console.warn(`enter 실패 user ${u.userId}: ${r.status}`);
  }

  return { users, chatId };
}

// =====================================================================
//  기본 함수: VU마다 WS 1개. 8081 / 8082 / 8083 에 균등 분배.
// =====================================================================
export default function (data) {
  const u = data.users[(__VU - 1) % data.users.length];
  const port = PORTS[(__VU - 1) % PORTS.length];
  const url = `ws://${HOST}:${port}/ws/minichat?token=${u.token}`;

  const socket = new WebSocket(url);

  socket.onopen = () => {
    if (__VU > TALKERS) return;    // 수신 전용 VU — 소켓만 열고 송신 안 함

    const timer = setInterval(() => {
      const payload = `{"chatId":${data.chatId},"type":"TALK","content":"{\\"t\\":${Date.now()},\\"v\\":${__VU}}"}`;
      socket.send(payload);
      sent.add(1);
    }, 1000);

    // 송신만 중단하고 소켓은 열어둔다 (드레인). 종료는 k6 duration이 담당.
    setTimeout(() => clearInterval(timer), SEND_MS);
  };

  socket.onmessage = (e) => {
    received.add(1);
    try {
      const msg = JSON.parse(e.data);
      if (msg.content) {
        const inner = JSON.parse(msg.content);
        if (inner.t) deliveryLatency.add(Date.now() - inner.t);
      }
    } catch (_) { /* 시스템 메시지 등은 무시 */ }
  };

  socket.onerror = (e) => console.error(`WS 에러 (VU ${__VU}): ${e.error}`);
}

export function handleSummary(data) {
  const m = data.metrics;
  const s = (m.app_msgs_sent && m.app_msgs_sent.values.count) || 0;
  const r = (m.app_msgs_received && m.app_msgs_received.values.count) || 0;
  const expected = s * USERS;
  const rate = expected > 0 ? (r / expected * 100).toFixed(2) + '%' : 'N/A';
  const line = `\n=== 전달률 ===\nsent=${s}  expected=${expected}  received=${r}  →  ${rate}\n`;
  return { stdout: textSummary(data, { indent: ' ', enableColors: true }) + line };
}