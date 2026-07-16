package com.dy.minichat.service;

import com.dy.minichat.entity.Message;
import com.dy.minichat.global.infra.kafka.payload.UserChatUpdatePayload;
import com.dy.minichat.global.infra.kafka.producer.UserChatUpdateProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class UserChatUpdateService {
    private final UserChatUpdateProducer userChatUpdateProducer;

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

    p.s redis pipeline
    get
    resp
    get
    resp
    get
    get
    get
    resp
    resp
    resp

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
        UserChatUpdatePayload event = UserChatUpdatePayload.builder()
                .chatId(chatId)
                .lastMessageId(lastMessage.getId())
                .timestamp(lastMessage.getCreatedAt())
                .build();

        userChatUpdateProducer.sendUserChatUpdateEvent(event);
    }
}