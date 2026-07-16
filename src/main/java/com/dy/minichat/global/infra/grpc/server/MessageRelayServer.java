package com.dy.minichat.global.infra.grpc.server;

import com.dy.grpc.proto.RelayBulkMessageRequest;
import com.dy.grpc.proto.RelayMessageResponse;
import com.dy.grpc.proto.RelayMessageServiceGrpc;
import com.dy.minichat.global.infra.websocket.WebSocketSessionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

@Slf4j
@GrpcService // gRPC 서비스임을 나타내는 어노테이션
@RequiredArgsConstructor
public class MessageRelayServer extends RelayMessageServiceGrpc.RelayMessageServiceImplBase {

    private final WebSocketSessionManager sessionManager;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    /**
     * [신규] 벌크 릴레이 RPC 구현
     */
    @Override
    public void relayBulkMessage(RelayBulkMessageRequest request,
                                 StreamObserver<RelayMessageResponse> responseObserver) {
        log.info("gRPC relayBulkMessage 요청 수신: senderId: {}, 수신자 {}명",
                request.getSenderId(), request.getRecipientIdsCount());

        String messagePayload;
        try {
            messagePayload = buildMessagePayload(request);
        } catch (JsonProcessingException e) {
            log.error("벌크 릴레이 메시지 직렬화 실패", e);
            responseObserver.onError(e);
            return;
        }

        TextMessage textMessage = new TextMessage(messagePayload);
        int deliveredCount = 0;
        int failedCount = 0;

        for (Long recipientId : request.getRecipientIdsList()) {
            WebSocketSession session = sessionManager.getSession(recipientId);

            if (session != null && session.isOpen()) {
                try {
                    session.sendMessage(textMessage);
                    deliveredCount++;
                    log.info("gRPC -> WebSocket 벌크 릴레이 성공. 수신자 ID: {}", recipientId);
                } catch (IOException e) {
                    log.error("gRPC -> WebSocket 벌크 전송 실패. 수신자 ID: {}", recipientId, e);
                    failedCount++;
                }
            } else {
                log.warn("벌크 릴레이 세션 없음. 수신자 ID: {}", recipientId);
                failedCount++;
            }
        }

        RelayMessageResponse response = RelayMessageResponse.newBuilder()
                .setSuccess(failedCount == 0)
                .setMessage(String.format("Bulk relay: %d delivered, %d failed", deliveredCount, failedCount))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // gRPC Request 객체로부터 WebSocket으로 보낼 JSON 문자열을 생성하는 헬퍼 메서드
    private String buildMessagePayload(RelayBulkMessageRequest request) throws JsonProcessingException {
        Map<String, Object> payloadMap = Map.of(
                "type", request.getMessageType(),
                "senderId", request.getSenderId(),
                "chatId", request.getChatId(),
                "content", request.getContent(),
                "timestamp", Instant.parse(request.getTimestamp())
        );
        return objectMapper.writeValueAsString(payloadMap);
    }
}
