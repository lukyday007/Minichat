import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// =====================================================================
// 실행 방법
//   k6 run -e MEMBERS=20 -e VUS=30  connection-pool-test.js
//   k6 run -e MEMBERS=20 -e VUS=60  connection-pool-test.js
//   k6 run -e MEMBERS=20 -e VUS=100 connection-pool-test.js
//   k6 run -e MEMBERS=50 -e VUS=30  connection-pool-test.js
//   k6 run -e MEMBERS=50 -e VUS=60  connection-pool-test.js
//   k6 run -e MEMBERS=50 -e VUS=100 connection-pool-test.js
//
// 측정 대상: createChat 트랜잭션
//   chat INSERT 1건 + userChat INSERT (MEMBERS)건 + message INSERT 1건
//   → MEMBERS를 키우면 트랜잭션당 커넥션 점유 시간이 길어짐
// =====================================================================

const SERVERS = [
  { http: 'http://localhost:8081', id: 'local1' },
  { http: 'http://localhost:8082', id: 'local2' },
  { http: 'http://localhost:8083', id: 'local3' },
];

const SIGNUP_PATH = '/minichat/user/auth/signup';
const LOGIN_PATH = '/minichat/user/auth/login';
const CREATE_CHAT_PATH = '/minichat/chat';

// 파라미터 (환경변수로 주입, 미지정 시 기본값)
const MEMBERS = parseInt(__ENV.MEMBERS || '20');
const VUS = parseInt(__ENV.VUS || '30');
const DURATION = __ENV.DURATION || '30s';

// 방 생성 사이 간격(초) — 0에 가까울수록 압박이 커짐
const THINK_TIME = 0.1;

// =====================================================================
// METRICS
// =====================================================================
const roomCreated = new Counter('pool_room_created');
const roomFailed = new Counter('pool_room_failed');
const createDuration = new Trend('pool_create_duration', true);

export const options = {
  scenarios: {
    heavy_tx: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '15s',
    },
  },
  // 실패율이 치솟으면 즉시 알 수 있도록
  thresholds: {
    'pool_room_failed': ['count<1000'],
  },
};

function jsonHeaders(token) {
  const h = { 'Content-Type': 'application/json' };
  if (token) h['Authorization'] = `Bearer ${token}`;
  return h;
}

// =====================================================================
// SETUP — 초대 대상 유저 풀 생성
// =====================================================================
export function setup() {
  const stamp = Date.now();
  const pool = [];

  console.log(`[setup] 유저 풀 ${MEMBERS}명 생성 중...`);

  for (let i = 0; i < MEMBERS; i++) {
    const server = SERVERS[i % SERVERS.length];
    const email = `pooltest_${stamp}_${i}@test.com`;
    const password = 'test1234!';

    http.post(
      `${server.http}${SIGNUP_PATH}`,
      JSON.stringify({ email, password, name: `풀테스터${i}` }),
      { headers: jsonHeaders() }
    );

    const loginRes = http.post(
      `${server.http}${LOGIN_PATH}`,
      JSON.stringify({ email, password }),
      { headers: jsonHeaders() }
    );

    let token = null;
    let userId = null;
    try {
      token = loginRes.json().accessToken;
      // Snowflake ID 정밀도 손실 방지 — 원문에서 문자열로 추출
      const m = loginRes.body.match(/"userId"\s*:\s*"?(\d+)"?/);
      userId = m ? m[1] : null;
    } catch (e) {
      console.error(`[setup] 로그인 실패 (user ${i}): ${loginRes.body}`);
    }

    pool.push({ token, userId });
  }

  const memberIds = pool.map((u) => u.userId).filter((id) => id !== null);
  console.log(`[setup] 완료 — 유효 유저 ${memberIds.length}명`);

  return { pool, memberIds };
}

// =====================================================================
// MAIN — 각 VU가 방 생성을 반복 (무거운 트랜잭션 동시 발생)
// =====================================================================
export default function (data) {
  // VU마다 다른 유저를 생성자로 사용 (토큰 재사용은 허용)
  const creator = data.pool[__VU % data.pool.length];
  const server = SERVERS[__VU % SERVERS.length];

  if (!creator || !creator.token) {
    console.error(`[VU ${__VU}] 생성자 토큰 없음 → 스킵`);
    return;
  }

  const res = http.post(
    `${server.http}${CREATE_CHAT_PATH}`,
    JSON.stringify({
      title: `풀테스트_VU${__VU}_${Date.now()}`,
      userIds: data.memberIds,
    }),
    {
      headers: jsonHeaders(creator.token),
      tags: { server: server.id },
    }
  );

  createDuration.add(res.timings.duration);

  const ok = check(res, { 'createChat 201': (r) => r.status === 201 });

  if (ok) {
    roomCreated.add(1);
  } else {
    roomFailed.add(1);
    // 커넥션 풀 고갈 시 나타나는 증상을 구분해서 기록
    if (res.status === 0) {
      console.error(`[VU ${__VU}] 응답 없음 (타임아웃 가능성)`);
    } else if (res.status >= 500) {
      console.error(`[VU ${__VU}] 서버 오류 ${res.status}: ${res.body}`);
    }
  }

  sleep(THINK_TIME);
}

// =====================================================================
// TEARDOWN
// =====================================================================
export function teardown(data) {
  console.log('==================================================');
  console.log(`실행 조건: MEMBERS=${MEMBERS}, VUS=${VUS}, DURATION=${DURATION}`);
  console.log(`트랜잭션당 INSERT 예상: ${MEMBERS + 2}건`);
  console.log('  (chat 1 + userChat ' + MEMBERS + ' + message 1)');
  console.log('--------------------------------------------------');
  console.log('Grafana 확인 항목:');
  console.log('  Active Connections — 풀 크기(10)에 붙는지');
  console.log('  Pending Threads    — 0을 넘는지 (넘으면 고갈)');
  console.log('  Usage Time         — 트랜잭션 점유 시간 변화');
  console.log('==================================================');
}
