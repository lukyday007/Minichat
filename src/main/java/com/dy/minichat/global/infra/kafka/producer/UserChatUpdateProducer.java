package com.dy.minichat.global.infra.kafka.producer;

import com.dy.minichat.global.infra.kafka.payload.UserChatUpdatePayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserChatUpdateProducer {
    private final KafkaTemplate<String, UserChatUpdatePayload> kafkaTemplate;
    private static final String TOPIC = "user-chat-update";

    public void sendUserChatUpdateEvent(UserChatUpdatePayload event) {
        kafkaTemplate.send(TOPIC, event);
        // log.info("[Kafka] UserChatUpdateEvent produced. ChatId={}", event.getChatId());
    }
}