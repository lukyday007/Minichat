package com.dy.minichat.global.infra.kafka.consumer;

import com.dy.minichat.dto.message.TalkMessageDTO;
import com.dy.minichat.global.infra.kafka.payload.ChatMessagePayload;
import com.dy.minichat.global.infra.websocket.WebSocketHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageConsumer {

    // WebSocketHandler를 주입받음
    private final WebSocketHandler webSocketHandler;

    @KafkaListener(
            topics = "chat-message",
            groupId = "chat-group"
    )
    public void consume(ChatMessagePayload event) {
        TalkMessageDTO message = event.getTalkMessage();
        if (message == null) {
            log.warn("[Kafka] Consumed null message event.");
            return;
        }

        // Handler의 public 메서드 호출
        webSocketHandler.broadcastMessage(message);
    }
}