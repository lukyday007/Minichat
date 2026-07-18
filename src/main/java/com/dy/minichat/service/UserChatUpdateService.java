package com.dy.minichat.service;

import com.dy.minichat.entity.Message;
import com.dy.minichat.global.infra.kafka.payload.UserChatUpdatePayload;
import com.dy.minichat.global.infra.kafka.producer.UserChatUpdateProducer;
import com.dy.minichat.repository.UserChatJdbcRepository;
import com.dy.minichat.repository.UserChatRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserChatUpdateService {
    private final UserChatUpdateProducer userChatUpdateProducer;

    private final UserChatRepository userChatRepository;
    private final UserChatJdbcRepository userChatJdbcRepository;


    /*
    1. 비동기
    2. 레디스
    3. 카프카
    4. 스레드플
    5. batchupdate
    update
    resp
    update
    resp * 1000
    batch update

        별도 스레드 비동기 실행
    */

    /*
        jpql 벌크 연산 적용 - 여러 개의 쿼리를 한꺼번에 = 레디스 파이프라인
        Query : UPDATE UserChat SET ... WHERE chat.id = ? AND is_deleted = false

        dirty checking
        : 로우 한건 업데이트

        ------------------

        jpql bulk update (modifying)
        : 로우 여러건을 한번에 업데이트하는 쿼리를 실행

        -------------------

        yaml jpql option (pipeline)
        : 여러개의 write 문을 한번에 redis pipeline 처럼 실행하고 싶을때 쓰는 옵션

        jdbc bulk update (pipeline)
        : 여러개의 write 문을 한번에 redis pipeline 처럼 실행하고 싶을때 쓰는 jdbc code
    */

    // 카프카 Async 둘 중 하나만 선택 -> only use kafka
    @Transactional
    public void updateUserChatOnNewMessage(Long chatId, Message lastMessage) {
        if (chatId == null || lastMessage == null) {
            log.error("[채팅 업데이트 실패] 필수 파라미터가 누락되었습니다. chatId: {}, lastMessage: {}", chatId, lastMessage);
            throw new IllegalArgumentException("채팅방 ID와 최신 메시지 엔티티는 필수값입니다.");
        }
        UserChatUpdatePayload event = UserChatUpdatePayload.builder()
                .chatId(chatId)
                .lastMessageId(lastMessage.getId())
                .timestamp(lastMessage.getCreatedAt())
                .build();

        userChatUpdateProducer.sendUserChatUpdateEvent(event);
    }

    // 테스트할 때 -> List<UserChatUpdateEvent> events 이렇게도 할 수 있음!
    @Transactional
    public void batchUpdateLastWrittenMessage(UserChatUpdatePayload event) {
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