package com.dy.minichat.global.infra.websocket;

import com.dy.minichat.dto.message.TalkMessageDTO;
import com.dy.minichat.dto.request.MessageRequestDTO;
import com.dy.minichat.global.infra.kafka.payload.ChatMessagePayload;
import com.dy.minichat.global.infra.grpc.client.MessageRelayClient;
import com.dy.minichat.global.infra.kafka.producer.ChatMessageProducer;
import com.dy.minichat.global.property.GrpcServerProperties;
import com.dy.minichat.service.ChatService;
import com.dy.minichat.service.FcmPushService;
import com.dy.minichat.service.MessageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;

    private final ChatService chatService;
    private final MessageService messageService;
    private final FcmPushService fcmPushService;

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;

    private final String serverIdentifier; // ServerConfig에서 생성된 Bean
    private static final String USER_SERVER_KEY_PREFIX = "ws:user:server:";
    private static final Duration USER_SERVER_TTL = Duration.ofHours(24);
    private static final Duration USER_STATE_TTL = Duration.ofHours(24);

    private final ChatMessageProducer chatMessageProducer;

    private final MessageRelayClient messageRelayClient; // gRPC 클라이언트 주입
    private final GrpcServerProperties grpcServerProperties; // gRPC 서버 주소록 주입

    /*
        웹소켓 세션을 중앙에서 관리하는 WebSocketSessionManager 주입
        private final Map<Long, WebSocketSession> userIdToSessionMap = new ConcurrentHashMap<>();
    */
    private final WebSocketSessionManager sessionManager;

    @Value("${relay.mode:bulk}")
    private String relayMode;

    private final AtomicLong mgetCircuitOpenUntil = new AtomicLong(0);
    private final AtomicLong chatIdCircuitOpenUntil = new AtomicLong(0);
    private static final long CIRCUIT_OPEN_MS = 5000;


    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {

        Optional<Long> userIdOptional = getUserIdFromSession(session);

        if (userIdOptional.isPresent()) {
            Long userId = userIdOptional.get();

            // Redis 성공 여부와 무관하게 로컬 세션은 반드시 등록
            sessionManager.addSession(userId, session);

            try {
                String userKey = "userId:" + userId + ":state";
                String chatIdStr = (String) redisTemplateForString.opsForHash().get(userKey, "chatId");

                if (chatIdStr != null) {
                    // 세션 캐시에 chatId 적재 → resolveChatId 폴백용
                    session.getAttributes().put("chatId", Long.parseLong(chatIdStr));

                    // redis - user : chat
                    redisTemplateForString.opsForHash().put(userKey, "serverId", serverIdentifier);
                    redisTemplateForString.opsForHash().put(userKey, "lastActive", LocalDateTime.now().toString());

                    // redis - user : server
                    String redisKey = USER_SERVER_KEY_PREFIX + userId;
                    redisTemplateForString.opsForValue().set(redisKey, serverIdentifier);

                    // 재연결 시 상태 키 TTL 갱신
                    redisTemplateForString.expire(userKey, USER_STATE_TTL);
                } else {
                    log.warn("유저 {}의 chatId 정보가 없음 — API 미호출 가능성 있음", userId);
                }

            } catch (Exception e) {
                log.error("[Fail-Open] 연결 수립 중 Redis 실패 — 로컬 세션만 유지. user: {}", userId, e);
            }

        } else {
            try {
                log.warn("세션에 userId 속성이 없어 연결을 종료합니다. (ID: {})", session.getId());
                session.close(CloseStatus.BAD_DATA.withReason("Invalid session: Missing userId"));
            } catch (Exception e) {
                log.error("세션 종료 중 에러 발생", e);
            }
        }
    }


    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {

        // 세션에서 보낸 사람의 ID를 안전하게 가져오기
        Optional<Long> senderIdOptional = getUserIdFromSession(session);
        if (senderIdOptional.isEmpty()) {
            log.warn("userId가 없는 비정상 세션(ID: {})으로부터 메시지 수신 시도. 무시합니다.", session.getId());
            return; // userId가 없으면 아무 처리도 하지 않음
        }
        Long senderId = senderIdOptional.get();

        String payload = message.getPayload();
        TalkMessageDTO talkMessageDTO = objectMapper.readValue(payload, TalkMessageDTO.class);

        Long chatId = resolveChatId(session, senderId, talkMessageDTO.getChatId());
        if (chatId == null) return;

        talkMessageDTO.setSenderId(senderId);
        talkMessageDTO.setTimestamp(Instant.now());

        switch (talkMessageDTO.getType()) {

            case TALK:
                // 선 DB 저장
                messageService.createMessage(
                        new MessageRequestDTO(talkMessageDTO.getContent()), senderId, chatId
                );

                // 카프카 패이로드 조립 후 프로듀서에게 위임
                // -> 컨슈머가 받아서 안정적 처리
                // => 만약 안되면? (그래서 먼저 디비에 저장)
                ChatMessagePayload event = ChatMessagePayload.builder()
                        .talkMessage(talkMessageDTO)
                        .build();
                chatMessageProducer.send(event);

                // log.info("[메시지] 보낸사람: {}, 채팅방: {}, 내용: {}", senderId, chatId, talkMessageDTO.getContent());
                break;

            // 향후 다른 실시간 메시지 타입(예: READ_ACK)이 추가.
            default:
                log.warn("처리할 수 없는 메시지 타입({}) 수신", talkMessageDTO.getType());
                break;
        }
    }


    /**
     * 발신자의 현재 chatId를 확정한다.
     * Redis(우선) → 세션 캐시(장애 시 폴백) 순으로 조회, 메시지의 chatId와 일치하는지 검증
     * @return 검증된 chatId. 확정 불가하거나 불일치하면 null (메시지 폐기)
     */
    private Long resolveChatId(WebSocketSession session, Long senderId, Long claimedChatId) {
        Long chatId = null;

        // 1) Redis 조회 (서킷이 닫혀 있을 때만) - write through
        long now = System.currentTimeMillis();
        if (now >= chatIdCircuitOpenUntil.get()) {
            try {
                Object v = redisTemplateForString.opsForHash()
                        .get("userId:" + senderId + ":state", "chatId");

                // 직전까지 정상 대화하던 유저는 장애 중에도 계속 전송 가능, 장애 중 방을 옮긴 유저만 차단
                if (v != null) {
                    chatId = Long.parseLong(v.toString());
                    session.getAttributes().put("chatId", chatId);
                }
            } catch (Exception e) {
                chatIdCircuitOpenUntil.set(now + CIRCUIT_OPEN_MS);
                log.error("[서킷 오픈] Redis chatId 조회 실패 → {}ms간 세션 캐시 사용. user:{}",
                        CIRCUIT_OPEN_MS, senderId, e);
            }
        }

        // 2) Redis 실패/서킷 오픈 → 세션 캐시 폴백
        if (chatId == null && session.getAttributes().get("chatId") instanceof Long cached) {
            chatId = cached;
        }

        // 3) 검증
        if (chatId == null) {
            log.warn("[메시지 무시] user:{} chatId 확정 실패 (Redis·세션 캐시 모두 없음)", senderId);
            return null;
        }
        if (!chatId.equals(claimedChatId)) {
            log.warn("[메시지 무시] user:{} 실제 chatId({}) ≠ 메시지 chatId({})",
                    senderId, chatId, claimedChatId);
            return null;
        }
        return chatId;
    }


    // [신규 추가] Kafka Consumer가 호출할 public 메서드 -> private sendMessageToChatRoom
    public void broadcastMessage(TalkMessageDTO message) {
        // log.info("[Kafka Consume] Broadcast 시작. ChatId: {}, Sender: {}", message.getChatId(), message.getSenderId());
        sendMessageToChatRoom(message);
    }


    @Qualifier("customThreadPool")
    private final Executor executor;

    // 특정 채팅방에 메시지를 방송하는 헬퍼 메서드
    private void sendMessageToChatRoom(TalkMessageDTO message) {
        Long chatId = message.getChatId();
        Set<Long> userIdsInChat = chatService.getUsersInChat(chatId);

        if (userIdsInChat == null || userIdsInChat.isEmpty()) {
            log.warn("메시지를 전송할 사용자가 없습니다. (채팅방 ID: {})", chatId);
            return;
        }

        String messagePayload;
        try {
            messagePayload = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("메시지 DTO JSON 변환 실패. ChatId: {}", chatId, e);
            return;
        }
        TextMessage textMessage = new TextMessage(messagePayload);

        // Phase 1: 유저 분류 — 로컬 세션 보유 여부로 분리
        List<Long> localUserIds = new ArrayList<>();
        List<Long> remoteOrOfflineUserIds = new ArrayList<>();
        classifyUsers(userIdsInChat, localUserIds, remoteOrOfflineUserIds);

        // Phase 2: 로컬 유저 — WebSocket 직접 전달
        sendToLocalUsers(localUserIds, textMessage);

        if (remoteOrOfflineUserIds.isEmpty()) {
            return;
        }

        // Phase 3~6: 원격/오프라인 유저 처리 (Redis 조회 → 서버별 릴레이 → 오프라인 저장)
        handleRemoteAndOfflineUsers(remoteOrOfflineUserIds, message, chatId);
    }


    // Phase 1: 유저 분류 — 로컬 세션 보유 여부로 분리
    private void classifyUsers(Set<Long> userIdsInChat, List<Long> localUserIds, List<Long> remoteOrOfflineUserIds) {
        for (Long userId : userIdsInChat) {
            WebSocketSession session = sessionManager.getSession(userId);
            if (session != null && session.isOpen()) {
                localUserIds.add(userId);
            } else {
                remoteOrOfflineUserIds.add(userId);
            }
        }
    }


    // Phase 2: 로컬 유저 — WebSocket 직접 전달
    private void sendToLocalUsers(List<Long> localUserIds, TextMessage textMessage) {
        for (Long userId : localUserIds) {
            executor.execute(() -> {
                WebSocketSession session = sessionManager.getSession(userId);

                if (session != null && session.isOpen()) {
                    try {
                        synchronized (session) {
                            session.sendMessage(textMessage);
                        }
                        // log.info("로컬 메시지 전송 성공. 수신자: {}", userId);

                    } catch (IOException e) {
                        log.error("메시지 전송 실패. 수신자: {}", userId, e);
                    }
                }
            });
        }
    }


    // Phase 3~6: 원격 유저 서버 위치 조회 → 서버별 gRPC 릴레이 → 오프라인 유저 저장/FCM
    private void handleRemoteAndOfflineUsers(List<Long> remoteOrOfflineUserIds, TalkMessageDTO message, Long chatId) {

        // Phase 3: Redis MGET — 원격 유저의 서버 위치를 한 번에 조회
        List<String> redisKeys = remoteOrOfflineUserIds.stream()
                .map(id -> USER_SERVER_KEY_PREFIX + id)
                .toList();

        List<String> serverIds;
        long now = System.currentTimeMillis();

        if (now < mgetCircuitOpenUntil.get()) {
            serverIds = null;   // 서킷 열림 → 전원 오프라인 처리
        } else {
            try {
                serverIds = redisTemplateForString.opsForValue().multiGet(redisKeys);
            } catch (Exception e) {
                mgetCircuitOpenUntil.set(System.currentTimeMillis() + CIRCUIT_OPEN_MS);
                log.error("[서킷 오픈] Redis 세션 위치 조회 실패 → 5초간 전원 오프라인 처리", e);
                serverIds = null;
            }
        }


        // Phase 4: 서버별 그룹핑
        Map<String, List<Long>> serverToRecipients = new HashMap<>();
        List<Long> offlineUserIds = new ArrayList<>();

        for (int i = 0; i < remoteOrOfflineUserIds.size(); i++) {
            Long userId = remoteOrOfflineUserIds.get(i);
            String targetServerId = (serverIds != null) ? serverIds.get(i) : null;

            if (targetServerId != null && !targetServerId.equals(serverIdentifier)) {
                serverToRecipients
                        .computeIfAbsent(targetServerId, k -> new ArrayList<>())
                        .add(userId);
            } else {
                offlineUserIds.add(userId);
            }
        }

        // Phase 5: 서버별 gRPC 릴레이 (relay.mode 로 단건/벌크 전환)
        boolean singleMode = "single".equalsIgnoreCase(relayMode);
        serverToRecipients.forEach((targetServerId, recipientIds) -> {
            if (singleMode) {
                // 단건: 수신자 1명당 gRPC 호출 1회 (N calls)
                for (Long recipientId : recipientIds) {
                    executor.execute(() ->
                            relaySingleMessageViaGrpc(targetServerId, message, recipientId)
                    );
                }
            } else {
                // 벌크: 대상 서버 1대당 gRPC 호출 1회 (K calls)
                executor.execute(() ->
                        relayBulkMessageViaGrpc(targetServerId, message, recipientIds)
                );
            }
        });

        // Phase 6: 오프라인 유저 — FCM
        if (!offlineUserIds.isEmpty()) {
            saveOfflineMessages(offlineUserIds, message);
        }
    }


    // Phase 6 세부: 오프라인 유저 미전달 시 FCM 발송
    private void saveOfflineMessages(List<Long> offlineUserIds, TalkMessageDTO message) {
        // 운영 시 개선 후보 : executor.execute()로 던지거나, FCM 배치 API(멀티캐스트) 적용
        offlineUserIds.forEach(userId ->
                fcmPushService.sendPushNotification(userId, message));
    }

    /*
     * 벌크 gRPC 릴레이를 위한 헬퍼 메서드
     */
    private void relayBulkMessageViaGrpc(String targetServerId, TalkMessageDTO messageDTO, List<Long> recipientIds) {
        Map<String, String> addresses = grpcServerProperties.getAddresses();
        String targetAddress = addresses.get(targetServerId);

        if (targetAddress == null || targetAddress.isEmpty()) {
            log.error("gRPC 벌크 릴레이 실패: 대상 서버 '{}'의 주소를 찾을 수 없습니다.", targetServerId);
            return;
        }

        try {
            String[] parts = targetAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            messageRelayClient.relayBulkMessageToServer(host, port, messageDTO, recipientIds);

        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.error("gRPC 벌크 릴레이 실패: 주소 형식 오류. 서버: '{}', 주소: {}", targetServerId, targetAddress, e);
        } catch (Exception e) {
            log.error("gRPC 벌크 릴레이 중 예상치 못한 에러. 대상 서버: {}", targetServerId, e);
        }
    }

    /*
     * 싱글 gRPC 릴레이를 위한 헬퍼 메서드
     */
    private void relaySingleMessageViaGrpc(String targetServerId, TalkMessageDTO messageDTO, Long recipientId) {
        String targetAddress = grpcServerProperties.getAddresses().get(targetServerId);

        if (targetAddress == null || targetAddress.isEmpty()) {
            log.error("gRPC 단건 릴레이 실패: 대상 서버 '{}'의 주소를 찾을 수 없습니다.", targetServerId);
            return;
        }

        try {
            String[] parts = targetAddress.split(":");
            messageRelayClient.relayMessageToServer(
                    parts[0], Integer.parseInt(parts[1]), messageDTO, recipientId);

        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.error("gRPC 단건 릴레이 실패: 주소 형식 오류. 서버: '{}', 주소: {}", targetServerId, targetAddress, e);
        } catch (Exception e) {
            log.error("gRPC 단건 릴레이 중 예상치 못한 에러. 서버: {}, 수신자: {}", targetServerId, recipientId, e);
        }
    }


    @Override
    public void afterConnectionClosed (WebSocketSession session, CloseStatus status) throws Exception {
        Optional<Long> userIdOptional = getUserIdFromSession(session);
        if (userIdOptional.isEmpty()) {
            log.warn("[연결 종료] 연결이 끊겼습니다. 상태: {}", status);
            return;
        }

        Long userId = userIdOptional.get();
        sessionManager.removeSession(userId);

        try {
            String userKey = "userId:" + userId + ":state";
            String chatIdStr = (String) redisTemplateForString.opsForHash().get(userKey, "chatId");
            if (chatIdStr != null) {
                redisTemplateForString.opsForSet().remove("chatId:" + chatIdStr + ":userId", String.valueOf(userId));
            }
            redisTemplateForString.delete(USER_SERVER_KEY_PREFIX + userId);
            redisTemplateForString.opsForHash().delete(userKey, "serverId", "lastActive");
        } catch (Exception e) {
            log.error("[연결 종료] Redis 정리 실패 — TTL(24h) 만료에 위임. user: {}", userId, e);
        }
    }


    private Optional<Long> getUserIdFromSession (WebSocketSession session) {
        try {
            Map<String, Object> attributes = session.getAttributes();
            Object userIdObj = attributes.get("userId");

            // 속성 자체가 없는 경우
            if (userIdObj == null) {
                log.error("세션(ID: {})에 'userId' 속성이 존재하지 않습니다. HandshakeInterceptor 설정을 확인하세요.", session.getId());
                return Optional.empty();
            }

            // userId 속성이 존재하고, Long 타입인지 확인
            if (userIdObj instanceof Long) {
                return Optional.of((Long) userIdObj);
            }

            // 속성이 없거나 타입이 맞지 않으면 빈 Optional 반환
            return Optional.empty();

        } catch (Exception e) {
            log.error("세션에서 userId를 추출하는 중 에러 발생. Session ID: {}", session.getId(), e);
            return Optional.empty(); // 예외 발생 시에도 안전하게 빈 Optional 반환
        }
    }


    private Optional<Long> getCurrentChatIdForUser(Long userId) {
        try {
            String userKey = "userId:" + userId + ":state"; // Hash: {chatId, serverId, lastActive}
            Object chatIdObj = redisTemplateForString.opsForHash().get(userKey, "chatId");
            if (chatIdObj == null) return Optional.empty();
            return Optional.of(Long.parseLong(chatIdObj.toString()));

        } catch (Exception e) {
            log.error("Redis에서 user:{} 의 chatId 조회 실패", userId, e);
            return Optional.empty();
        }
    }
}