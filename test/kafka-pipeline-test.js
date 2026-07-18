import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

// =====================================================================
// CONFIG — 환경에 맞게 수정
// =====================================================================
const SERVERS = [
  { http: 'http://localhost:8081', ws: 'ws://localhost:8081', id: 'local1' },
  { http: 'http://localhost:8082', ws: 'ws://localhost:8082', id: 'local2' },
  { http: 'http://localhost:8083', ws: 'ws://localhost:8083', id: 'local3' },
];

// WebSocket 엔드포인트 경로 (WebSocketConfig의 registry.addHandler 경로와 일치해야 함)
const WS_PATH = '/ws/minichat';

// 인증 관련 경로 (UserController 실제 매핑에 맞게 수정)
const SIGNUP_PATH = '/minichat/user/auth/signup';
const LOGIN_PATH = '/minichat/user/auth/login';

const ROOM_COUNT = 5;
const USERS_PER_ROOM = 4;
const TOTAL_USERS = ROOM_COUNT * USERS_PER_ROOM; // 20

// 각 유저가 보낼 TALK 메시지 수 / 전송 간격(ms)
const TALK_PER_USER = 3;
const TALK_INTERVAL_MS = 1000;

// WebSocket 연결 유지 시간(ms) — 전파 대기 포함
const WS_HOLD_MS = 15000;

// =====================================================================
// METRICS
// =====================================================================
const talkSent = new Counter('pipeline_talk_sent');
const talkReceived = new Counter('pipeline_talk_received');
const systemEntryReceived = new Counter('pipeline_system_entry_received');
const systemLeaveReceived = new Counter('pipeline_system_leave_received');
const unknownReceived = new Counter('pipeline_unknown_received');

export const options = {
  scenarios: {
    pipeline: {
      executor: 'per-vu-iterations',
      vus: TOTAL_USERS,
      iterations: 1,
      maxDuration: '2m',
    },
  },
};

// =====================================================================
// HELPERS
// =====================================================================
function jsonHeaders(token) {
  const h = { 'Content-Type': 'application/json' };
  if (token) h['Authorization'] = `Bearer ${token}`;
  return h;
}

// 같은 방 유저를 서로 다른 서버에 분산 → gRPC 릴레이 경로 강제 발생
function serverForUser(userIndex) {
  return SERVERS[userIndex % SERVERS.length];
}

function roomForUser(userIndex) {
  return Math.floor(userIndex / USERS_PER_ROOM);
}

// =====================================================================
// SETUP — 유저 생성/로그인 → 방 5개 생성 → chatId 수집
// =====================================================================
export function setup() {
  const stamp = Date.now();
  const users = [];

  // ---- 1. 회원가입 + 로그인 ----
  for (let i = 0; i < TOTAL_USERS; i++) {
    const server = serverForUser(i);
    const email = `loadtest_${stamp}_${i}@test.com`;
    const password = 'test1234!';
    const name = `테스터${i}`;

    const signupRes = http.post(
      `${server.http}${SIGNUP_PATH}`,
      JSON.stringify({ email, password, name }),
      { headers: jsonHeaders() }
    );
    check(signupRes, { 'signup ok': (r) => r.status === 200 || r.status === 201 });

    const loginRes = http.post(
      `${server.http}${LOGIN_PATH}`,
      JSON.stringify({ email, password }),
      { headers: jsonHeaders() }
    );
    check(loginRes, { 'login ok': (r) => r.status === 200 || r.status === 201 });

    let token = null;
    let userId = null;
    try {
      const body = loginRes.json();
      token = body.accessToken;
      // userId는 정밀도 손실 방지를 위해 원문에서 문자열로 추출
      const m = loginRes.body.match(/"userId"\s*:\s*"?(\d+)"?/);
      userId = m ? m[1] : null;
    } catch (e) {
      console.error(`[setup] 로그인 응답 파싱 실패 (user ${i}): ${loginRes.body}`);
    }

    users.push({ index: i, email, token, userId, serverIdx: i % SERVERS.length });
  }

  // ---- 2. 방 생성 (각 방의 첫 번째 유저가 생성, 나머지 3명 초대) ----
  const rooms = [];
  for (let r = 0; r < ROOM_COUNT; r++) {
    const creator = users[r * USERS_PER_ROOM];
    const memberIds = [];
    for (let m = 1; m < USERS_PER_ROOM; m++) {
      memberIds.push(users[r * USERS_PER_ROOM + m].userId);
    }

    const server = SERVERS[creator.serverIdx];
    const createRes = http.post(
      `${server.http}/minichat/chat`,
      JSON.stringify({ title: `부하테스트방_${r}`, userIds: memberIds }),
      { headers: jsonHeaders(creator.token) }
    );
    check(createRes, { 'createChat 201': (res) => res.status === 201 });

    // createChat 응답에 chatId가 없으면 GET /chats로 우회 조회
    // Snowflake ID 정밀도 손실 방지를 위해 원문에서 문자열로 추출
    let chatId = null;
    const createMatch = createRes.body.match(/"id"\s*:\s*"?(\d+)"?/);
    if (createMatch) chatId = createMatch[1];

    if (chatId === null) {
      const listRes = http.get(`${server.http}/minichat/chats`, {
        headers: jsonHeaders(creator.token),
      });
      const listMatch = listRes.body.match(/"(?:chatId|id)"\s*:\s*"?(\d+)"?/);
      if (listMatch) {
        chatId = listMatch[1];
      } else {
        console.error(`[setup] 채팅방 목록 파싱 실패 (room ${r}): ${listRes.body}`);
      }
    }

    console.log(`[setup] room ${r} → chatId=${chatId}, members=${memberIds.length + 1}`);
    rooms.push(chatId);
  }

  return { users, rooms, stamp };
}

// =====================================================================
// MAIN — 유저별 입장 → WebSocket 연결 → TALK 송수신
// =====================================================================
export default function (data) {
  const userIndex = __VU - 1;
  const user = data.users[userIndex];
  const chatId = data.rooms[roomForUser(userIndex)];
  const server = SERVERS[user.serverIdx];

  if (!user || !user.token || chatId === null || chatId === undefined) {
    console.error(`[VU ${__VU}] setup 데이터 누락 → 종료 (token=${!!user?.token}, chatId=${chatId})`);
    return;
  }

  // ---- 1. 채팅방 입장 (Redis 상태 등록) — WebSocket 연결 전 필수 ----
  const enterRes = http.post(
    `${server.http}/minichat/chat/${chatId}/enter`,
    null,
    { headers: jsonHeaders(user.token) }
  );
  check(enterRes, { 'enter 201': (r) => r.status === 201 });

  if (enterRes.status !== 201) {
    console.error(`[VU ${__VU}] 입장 실패 (${enterRes.status}): ${enterRes.body}`);
    return;
  }

  // 전체 VU가 입장을 마칠 때까지 잠시 대기 (SYSTEM_ENTRY 수신 누락 방지)
  sleep(2);

  // ---- 2. WebSocket 연결 ----
  const wsUrl = `${server.ws}${WS_PATH}`;
  const wsParams = {
    headers: { Authorization: `Bearer ${user.token}` },
    tags: { server: server.id },
  };

  const res = ws.connect(wsUrl, wsParams, function (socket) {
    socket.on('open', function () {
      console.log(`[VU ${__VU}] WS 연결 성공 (server=${server.id}, chatId=${chatId})`);

      // ---- 3. TALK 메시지 주기적 전송 ----
      let sentCount = 0;
      socket.setInterval(function () {
        if (sentCount >= TALK_PER_USER) {
          return; // k6/ws에는 clearInterval이 없어 플래그로 중단
        }
        sentCount++;

        const payload = JSON.stringify({
          chatId: chatId,
          type: 'TALK',
          content: `[VU${__VU}] 메시지 ${sentCount} @${Date.now()}`,
        });

        socket.send(payload);
        talkSent.add(1);
      }, TALK_INTERVAL_MS);
    })

    // ---- 4. 수신 메시지 타입별 집계 ----
    socket.on('message', function (raw) {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch (e) {
        console.error(`[VU ${__VU}] 수신 메시지 파싱 실패: ${raw}`);
        return;
      }

      switch (msg.type) {
        case 'TALK':
          talkReceived.add(1);
          break;
        case 'SYSTEM_ENTRY':
          systemEntryReceived.add(1);
          console.log(`[VU ${__VU}] SYSTEM_ENTRY 수신: ${msg.content}`);
          break;
        case 'SYSTEM_LEAVE':
          systemLeaveReceived.add(1);
          console.log(`[VU ${__VU}] SYSTEM_LEAVE 수신: ${msg.content}`);
          break;
        default:
          unknownReceived.add(1);
          console.warn(`[VU ${__VU}] 알 수 없는 타입 수신: ${raw}`);
      }
    });

    socket.on('error', function (e) {
      console.error(`[VU ${__VU}] WS 에러: ${e.error()}`);
    });

    socket.on('close', function () {
      console.log(`[VU ${__VU}] WS 연결 종료`);
    });

    // ---- 5. 시스템 메시지 검증: 방의 마지막 유저가 퇴장 ----
    if (userIndex % USERS_PER_ROOM === USERS_PER_ROOM - 1) {
      socket.setTimeout(function () {
        const leaveRes = http.del(
          `${server.http}/minichat/chats/${chatId}/leave`,
          null,
          { headers: jsonHeaders(user.token) }
        );
        console.log(`[VU ${__VU}] 퇴장 요청 → ${leaveRes.status}`);
      }, 8000);
    }

    // ---- 6. 전파 대기 후 종료 ----
    socket.setTimeout(function () {
      socket.close();
    }, WS_HOLD_MS);
  });

  check(res, { 'ws 101 handshake': (r) => r && r.status === 101 });
}

// =====================================================================
// TEARDOWN — 정합성 정산용 기대값 출력
// =====================================================================
export function teardown(data) {
  const expectedTalkSent = TOTAL_USERS * TALK_PER_USER;
  const expectedTalkReceived = expectedTalkSent * USERS_PER_ROOM;
  const expectedSystemEntry = ROOM_COUNT * USERS_PER_ROOM;

  console.log('==================================================');
  console.log('정합성 기대값 (실측치는 아래 metrics 참고)');
  console.log(`  TALK 전송 기대     : ${expectedTalkSent}`);
  console.log(`  TALK 수신 기대     : ${expectedTalkReceived} (방 인원 ${USERS_PER_ROOM}명 전원 수신 기준)`);
  console.log(`  SYSTEM_ENTRY 기대  : ${expectedSystemEntry} 이상`);
  console.log('==================================================');
  console.log('Kafka offset 확인:');
  console.log('  docker exec -it <kafka컨테이너> kafka-run-class.sh kafka.tools.GetOffsetShell \\');
  console.log('    --broker-list localhost:9092 --topic chat-message');
  console.log('==================================================');
}
