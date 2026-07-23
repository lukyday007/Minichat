package com.dy.minichat.global.infra.kafka.producer;

import com.dy.minichat.global.infra.kafka.payload.ChatMessagePayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageProducer {
    // KafkaTemplate<String, MessageSendEvent> 빈이 등록되어 있어야 함
    private final KafkaTemplate<String, ChatMessagePayload> kafkaTemplate;
    private static final String TOPIC = "chat-message"; // (토픽 이름)

    public void send(ChatMessagePayload event) {
        try {
            // TalkMessageDTO의 chatId를 Key로 사용 (순서 보장)
            String key = String.valueOf(event.getTalkMessage().getChatId());
            kafkaTemplate.send(TOPIC, key, event);
            // log.info("[Kafka] Chat message produced. Key: {}", key);
        } catch (Exception e) {
            log.error("[Kafka] Produce 실패: {}", event.getTalkMessage().getContent(), e);
        }
    }
}