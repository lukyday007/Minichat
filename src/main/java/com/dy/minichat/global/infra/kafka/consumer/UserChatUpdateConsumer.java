package com.dy.minichat.global.infra.kafka.consumer;

import com.dy.minichat.global.infra.kafka.payload.UserChatUpdatePayload;
import com.dy.minichat.repository.UserChatJdbcRepository;
import com.dy.minichat.repository.UserChatRepository;
import com.dy.minichat.service.UserChatUpdateService;
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

    private final UserChatUpdateService userChatUpdateService;

    @KafkaListener(
            topics = "user-chat-update",
            groupId = "chat-service-group")
    public void consume(UserChatUpdatePayload event) {
        userChatUpdateService.batchUpdateLastWrittenMessage(event);
    }
}