# 🚀 MiniChat - 실시간 채팅 서비스

> 📌 English version is available below. Scroll down to view the English README.

> **실시간 채팅 서비스에서 발생하는 동시성·정합성·장애 문제를 데이터 특성 기반으로 판단하고 해결한 개인 프로젝트**
> 
> <br>
> Master-Replica DB 구조와 Redis/Kafka 분산 처리 아키텍처를 도입하여, 실시간성과 대용량 트래픽 처리에 최적화된 서비스를 구현했습니다.

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

## 💡 Key Features (구현 기능)

### 1. DB 부하 분산 및 트랜잭션 최적화
* **Master-Replica 구조:** `AbstractRoutingDataSource`를 활용하여 쓰기 작업(Master)과 읽기 작업(Replica)의 트랜잭션을 분리, DB 부하를 효과적으로 분산했습니다.
* **JPA 최적화:** 복합 인덱스 및 Pagination을 적용하여 대량의 데이터 조회 시 API 평균 응답 시간을 최적화했습니다.

### 2. 동시성 제어 (Concurrency Control)
* **Redis Lua Script:** 원자성(Atomicity)을 보장하는 Lua Script를 활용하여 고성능 Rate Limiter를 구현했습니다.
* **낙관적 락(Optimistic Lock):** 조건부 업데이트 로직을 통해 데이터 경합 상황에서도 정합성을 유지하도록 설계했습니다.

### 3. 고성능 실시간 통신
* **WebSocket & gRPC:** WebSocket으로 클라이언트와 연결하고, 서버 간 메시지 릴레이에는 Protobuf 기반의 gRPC를 사용하여 네트워크 오버헤드를 최소화하고 지연 시간(Latency)을 줄였습니다.
* **분산 세션 관리:** Redis를 활용하여 다중 서버 환경에서도 끊김 없는 채팅 세션을 유지합니다.

### 4. 비동기 이벤트 처리 (Event-Driven)
* **Kafka 활용:** 채팅 전송과 채팅방 목록 업데이트 로직을 Kafka 이벤트로 분리하여 서비스 간 결합도를 낮추고(Decoupling), 시스템의 확장성을 확보했습니다.

<br>

## 📈 Performance & Improvements (성능 개선 및 배운 점)

| 이슈 및 목표 | 해결 과정 및 결과 |
| :--- | :--- |
| **알고리즘 최적화** | 기존 연산 로직을 **누적 합계 알고리즘**으로 개선하여 연산 효율 극대화 |
| **쿼리 튜닝** | 조회 쿼리에 **복합 인덱스** 적용 및 페이징 처리를 통해 쿼리 실행 계획 최적화 |
| **캐싱 전략 고도화** | 빈번한 쓰기 작업에 대해 **Redis Write-Back** 전략을 도입, DB 부하 감소 및 응답 속도 개선 |
| **네트워크 비용** | JSON 기반 통신 대비 **gRPC** 도입으로 데이터 직렬화 크기를 줄여 릴레이 성능 최적화 |


--- 
# 🚀 MiniChat - Real-Time Chat Server for Large-Scale Traffic

> **A backend server designed for large-scale concurrent environments with a strong focus on data consistency and system stability**
> <br>
> Built with a Master-Replica database architecture and distributed processing using Redis and Kafka to optimize real-time communication and high-throughput traffic handling.



## 💡 Key Features

### 1. DB Load Distribution & Transaction Optimization
* **Master-Replica Architecture:** Separated write operations (Master) and read operations (Replica) using `AbstractRoutingDataSource` to effectively distribute database load.
* **JPA Optimization:** Applied composite indexes and pagination to optimize API response times for large-scale data retrieval.

### 2. Concurrency Control
* **Redis Lua Script:** Implemented a high-performance rate limiter using Lua Scripts to guarantee atomic operations.
* **Optimistic Locking:** Designed conditional update logic to maintain data consistency under concurrent access situations.

### 3. High-Performance Real-Time Communication
* **WebSocket & gRPC:** Used WebSocket for client communication and Protobuf-based gRPC for server-to-server message relay to minimize network overhead and reduce latency.
* **Distributed Session Management:** Leveraged Redis to maintain seamless chat sessions across multiple server instances.

### 4. Asynchronous Event Processing (Event-Driven)
* **Kafka Integration:** Decoupled chat delivery and chat room update logic through Kafka-based event processing to improve scalability and reduce service coupling.

<br>

## 📈 Performance Optimizations & Lessons Learned

| Issue / Goal | Optimization & Result |
| :--- | :--- |
| **Algorithm Optimization** | Improved computational efficiency by redesigning logic with a **prefix sum algorithm** |
| **Query Tuning** | Optimized query execution plans using **composite indexes** and pagination |
| **Caching Strategy** | Applied a **Redis Write-Back** strategy for write-heavy workloads to reduce DB load and improve response speed |
| **Network Overhead** | Reduced serialization payload size and improved relay performance by replacing JSON-based communication with **gRPC** |
