package com.dy.minichat.service;

import com.dy.minichat.repository.UserChatJdbcRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class LastReadBatchWriter {

    private final UserChatJdbcRepository userChatJdbcRepository;

    // 별도 빈이므로 프록시를 타고, 이 배치 하나가 단일 트랜잭션으로 커밋
    // 실패 시 예외를 호출자(스케줄러)로 던져 Dirty Set 되돌리기 유도
    @Transactional
    public void writeBatch(List<UserChatJdbcRepository.UserChatUpdate> batch) {
        userChatJdbcRepository.batchUpdateLastRead(batch);
    }
}