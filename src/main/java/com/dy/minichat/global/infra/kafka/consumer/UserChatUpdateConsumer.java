package com.dy.minichat.global.infra.kafka.consumer;

import com.dy.minichat.global.infra.kafka.payload.UserChatUpdatePayload;
import com.dy.minichat.repository.UserChatJdbcRepository;
import com.dy.minichat.repository.UserChatRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserChatUpdateConsumer {   // DB 업데이트 담당
    private final UserChatRepository userChatRepository;
    private final UserChatJdbcRepository userChatJdbcRepository;

    @KafkaListener(
            topics = "user-chat-update",
            groupId = "chat-service-group")
    @Transactional // 컨슈머에 어노테이션을 다는 것 보다 서비스에서 다는게 일반적
    // 테스트할 때 -> List<UserChatUpdateEvent> events 이렇게도 할 수 있음!
    public void consume(UserChatUpdatePayload event) {
        List<Long> userChatIds = userChatRepository.findIdsByChatId(event.getChatId());
        if (userChatIds.isEmpty()) return;

        userChatJdbcRepository.batchUpdateLastWrittenMessage(
                userChatIds,
                event.getLastMessageId(),
                event.getTimestamp()
        );
        log.info("[Kafka] UserChatUpdateEvent consumed. ChatId={}, Updated={}", event.getChatId(), userChatIds.size());
    }
}