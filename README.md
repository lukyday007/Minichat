# 🚀 MiniChat - 실시간 채팅 서비스

> 📌 English version is available below. Scroll down to view the English README.

> **대규모 동시 접속 환경을 고려하여 동시성, 데이터 정합성, 시스템 회복탄력성(Resilience)을 검증한 실시간 채팅 프로젝트**
>
> Redis 분산 세션과 gRPC 기반 라우팅, Kafka 비동기 이벤트를 활용하여 확장 가능한 분산 구조를 설계하고, k6 부하 테스트 및 장애 주입실험을 통해 시스템 안정성을 검증했습니다.

<br>

## 🛠 Architecture
<img src="img/minichat-architecture.jpeg" width="800" alt="Minichat Architecture">

<br>

## 📚 Tech Stack

### Backend & Database
<img src="https://img.shields.io/badge/Java_17-007396?style=for-the-badge&logo=java&logoColor=white" /> <img src="https://img.shields.io/badge/Spring_Boot_3.x-6DB33F?style=for-the-badge&logo=springboot&logoColor=white" /> <img src="https://img.shields.io/badge/JPA-59666C?style=for-the-badge&logo=hibernate&logoColor=white" /> <img src="https://img.shields.io/badge/MySQL_8.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white" />

### Messaging & Cache
<img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" /> <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" />

### Protocol
<img src="https://img.shields.io/badge/gRPC-244C5A?style=for-the-badge&logo=grpc&logoColor=white" /> <img src="https://img.shields.io/badge/WebSocket-010101?style=for-the-badge&logo=socket.io&logoColor=white" />

### Infrastructure & Monitoring
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" /> <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" /> <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white" />

<br>

## 💡 Key Features & Architecture

### 1. 분산 세션 관리 및 서버 간 통신
* **Redis 분산 세션:** 다중 WAS 환경에서 WebSocket 연결 시 수신자 위치를 식별하도록 세션 정보를 관리합니다.
* **gRPC 기반 라우팅:** 서버 간 메시지 릴레이 시 Protobuf 기반 gRPC를 적용하여 대상 서버 단위로 분산 처리합니다.

### 2. 비동기 이벤트 분리 및 데이터 일관성
* **Kafka 이벤트 분리:** 메시지 전송과 채팅방 목록/메타데이터 갱신 로직을 비동기 이벤트로 분리하여 서비스 간 결합도를 낮추었습니다.
* **데이터 특성별 일관성 분리:** 즉시 전달이 필요한 실시간 메시지와 최종 일관성(Eventual Consistency)이 허용되는 메타데이터의 저장/처리 파이프라인을 구분했습니다.

### 3. 동시성 제어 (Concurrency Control)
* **원자적 연산 (Lua Script):** Redis Lua Script를 활용하여 원자성(Atomicity)을 보장하는 Rate Limiter를 구현했습니다.
* **조건부 갱신 (Optimistic Locking):** DB 레벨의 조건부 업데이트를 통해 데이터 경합 상황에서 정합성을 유지하도록 설계했습니다.

### 4. DB 부하 분산
* **Master-Replica 구조:** `AbstractRoutingDataSource`를 활용하여 쓰기 작업(Master)과 읽기 작업(Replica) 트랜잭션을 분리하고 복제 지연 상황을 고려해 설계했습니다.

### 5. 부하 테스트 및 장애 주입 검증 (Load & Fault Injection Testing)
* **시스템 한계 측정 (k6):** 지속 부하 및 피크 트랙픽 시나리오를 적용하여 Knee Point와 병목 구간(Thread Pool, Network I/O)을 측정했습니다.
* **장애 대응 검증 (Chaos Engineering):** 부하 상황에서 Redis 중단 등 주요 의존성 장애를 주입하고, 타임아웃 단축·서킷 브레이커·DB 폴백을 적용하여 메시지 유실 없는 Graceful Degradation을 검증했습니다.


<br>

### ⚙️ Problem Solving & Engineering Trade-offs

프로젝트를 진행하며 직면한 구조적 문제와 부하/장애 테스트 기반의 기술적 의사결정 과정을 기록했습니다.

- [분산 락 대신 Lua Script를 선택한 이유](https://lukyday-blog.vercel.app/problem-solving/minichat/01-distributed-lock-vs-lua-script/)
- [Kafka 순서를 보장할 때와 포기할 때](https://lukyday-blog.vercel.app/problem-solving/minichat/02-kafka-message-ordering/)
- [복제 지연 상황에서 읽기 전략을 선택하는 기준](https://lukyday-blog.vercel.app/problem-solving/minichat/03-read-write-replication-lag/)
- [같은 Redis 장애에 다른 대응을 선택한 이유](https://lukyday-blog.vercel.app/problem-solving/minichat/04-redis-rate-limiter/)
- [분산 환경에서 ID 전략](https://lukyday-blog.vercel.app/problem-solving/minichat/05-id-strategy-in-distributed-system/)
- [WebSocket에서 브로드캐스트를 포기한 이유](https://lukyday-blog.vercel.app/problem-solving/minichat/06-websocket-scale-out-grpc/)
- [유저 단위보다 서버 단위를 선택한 이유](https://lukyday-blog.vercel.app/problem-solving/minichat/07-parallel-wasnt-enough/)

--- 

# 🚀 MiniChat - Real-Time Chat Server

> **A real-time chat backend focused on verifying concurrency, data consistency, and system resilience under large-scale concurrent environments.**
> 
> Designed a scalable distributed architecture using Redis distributed sessions, gRPC target routing, and Kafka asynchronous event pipelines, validated via k6 load testing and dependency fault injection.


<br>

## 💡 Key Features & Architecture

### 1. Distributed Session Management & Inter-Server Communication
* **Redis Distributed Sessions:** Tracks user connection states across multiple application instances for WebSocket scale-out.
* **gRPC Target Routing:** Relays messages between servers using Protobuf-based gRPC routing structured at the target server level.

### 2. Event Decoupling & Consistency Strategies
* **Kafka Event Pipeline:** Decoupled real-time message delivery from chat room metadata updates using asynchronous Kafka events.
* **Consistency Segregation:** Separated immediate real-time delivery paths from eventual consistency data pipelines.

### 3. Concurrency Control
* **Atomic Operations (Lua Script):** Implemented a rate limiter using Redis Lua Scripts to guarantee atomic execution.
* **Conditional Updates (Optimistic Locking):** Utilized conditional DB update statements to maintain data consistency under race conditions.

### 4. Database Load Distribution
* **Master-Replica Architecture:** Separated write operations (Master) and read operations (Replica) using `AbstractRoutingDataSource`.

### 5. Load & Fault Injection Testing
* **System Capacity Measurement (k6):** Applied sustained load and peak traffic scenarios to identify system knee points and thread pool I/O bottlenecks.
* **Fault Injection Validation:** Simulated dependency outages (e.g., Redis downtime) during peak load to verify zero-data-loss graceful degradation via timeouts, circuit breakers, and database fallbacks.

<br>

### ⚙️ Problem Solving & Engineering Trade-offs

A record of the structural challenges encountered during the project and the technical decisions made to resolve them.

- [Choosing Lua Script Over Distributed Lock](https://lukyday-blog.vercel.app/en/problem-solving/minichat/01-distributed-lock-vs-lua-script/)
- [When Kafka Ordering Matters and When It Doesn't](https://lukyday-blog.vercel.app/en/problem-solving/minichat/02-kafka-message-ordering/)
- [Choosing a Read Strategy Under Replication Lag](https://lukyday-blog.vercel.app/en/problem-solving/minichat/03-read-write-replication-lag/)
- [Why the Same Redis Failure Needs Different Responses](https://lukyday-blog.vercel.app/en/problem-solving/minichat/04-redis-rate-limiter/)
- [Choosing an ID Strategy for Distributed Systems](https://lukyday-blog.vercel.app/en/problem-solving/minichat/05-id-strategy-in-distributed-system/)
- [Choosing Target Routing Over Broadcast](https://lukyday-blog.vercel.app/en/problem-solving/minichat/06-websocket-scale-out-grpc/)
- [Choosing Server-Level Bulk Relay Over Per-User Processing](https://lukyday-blog.vercel.app/en/problem-solving/minichat/07-parallel-wasnt-enough/)
