package com.dy.minichat.service;

import com.dy.minichat.entity.UserStatus;
import com.dy.minichat.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserBanService { // UserBanServiceCGlibProxy...
    private final UserRepository userRepository;

    private static final Duration STRIKE_WINDOW = Duration.ofDays(30);

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    // (실제 구현) Redis나 DB에 사용자 밴 상태를 영구 저장

    // 1. 누적 위반 횟수 (증가/INCR)
    private static final String STRIKE_COUNT_KEY_PREFIX = "ban:strikes:user:";
    // 2. 임시 밴 상태 (키/값 + TTL)
    private static final String TEMP_BAN_KEY_PREFIX = "ban:state:user:";

    /**
     * [호출] RateLimitAspect
     * 사용자의 위반 횟수를 누적하고, 횟수에 따라 밴을 적용합니다.
     */
    // @Transactional(propagation = Propagation.REQUIRES_NEW) // Strike 3에서 DB 업데이트를 포함하므로 트랜잭션 처리
    // @Transactional Aspect Proxy (그래서 메서드 내부호출은 당연히 프록시(부가기능) 적용이 안됨)

    // AService 에도 @Tx BService 에도 @Tx 줄줄이 @Tx 만나면 어떻게되지?
    // @Tx 기본동작은 required (있으면 쓰고 없으면 내가연다)
    // 근데 내가 새롭게 열고싶을땐? required_new 로 새로운 @tx 연다고 명시적으로 옵션을 준다.
    @Transactional
    public void applyStrike(Long userId) {
        String strikeKey = STRIKE_COUNT_KEY_PREFIX + userId;
        String tempBanKey = TEMP_BAN_KEY_PREFIX + userId;

        // 1. 위반 횟수 1 증가 (INCR)
        Long strikeCount = redisTemplateForString.opsForValue().increment(strikeKey);
        if (strikeCount == null) {  // 레디스 다운 등으로 미응답 가능성 있음 - 예외처리
            log.error("[Strike] Redis increment 결과가 null입니다. 밴 처리를 건너뜁니다. userId: {}", userId);
            return;
        }

        if (strikeCount == 1) {
            // 첫 위반 시점부터 30일간 카운트 유지 (이후 자동 초기화)
            redisTemplateForString.expire(strikeKey, STRIKE_WINDOW);

            // [Strike 1] 1일 밴 적용
            log.warn("!!! [Strike 1] 사용자 {} 밴 처리 (1일)", userId);
            // SET ban:state:user:123 - STRIKE_1 - EX 86400
            redisTemplateForString.opsForValue().set(tempBanKey, "STRIKE_1", 1, TimeUnit.DAYS);

        } else if (strikeCount == 2) {
            // [Strike 2] 1주일 밴 적용
            log.warn("!!! [Strike 2] 사용자 {} 밴 처리 (1주일)", userId);
            // SET ban:state:user:123 - STRIKE_2 - EX 604800
            redisTemplateForString.opsForValue().set(tempBanKey, "STRIKE_2", 7, TimeUnit.DAYS);

        } else {
            // [Strike 3] 영구 밴 적용
            log.warn("!!! [Strike 3] 사용자 {} 밴 처리 (영구)", userId);

            // 1. RDB에 영구 밴 상태 업데이트 (메서드 직접 호출)
            this.banUser(userId); // -> @Tx 필요 -> 트랜잭션 범위가 큼
            // userService.banUser(userId); -> applyStrike @Tx 필요 없음 -> 트랜잭션 범위 작음

            // 2. 불필요해진 임시 밴 키, 스트라이크 키 삭제
            redisTemplateForString.delete(List.of(strikeKey, tempBanKey));
        }
    }


    /**
     * 사용자를 밴 처리 (DB 업데이트)
     */
    @Transactional
    public void banUser(Long userId) {
        userRepository.findById(userId).ifPresentOrElse(user -> {
            if (user.getUserStatus() == UserStatus.BAN) {
                log.info("[UserBanService] 이미 밴된 사용자입니다: {}", userId);
                return;
            }
            user.setUserStatus(UserStatus.BAN);
            log.warn("사용자 {} 밴 처리 완료 (DB 업데이트)", userId);
        }, () -> {
            log.warn("[UserBanService] 존재하지 않는 사용자: {}", userId);
        });
    }


    /**
     * [호출] HandshakeInterceptor 또는 로그인 API
     * 사용자가 밴 상태인지 (임시 밴 or 영구 밴) 확인합니다.
     */
    // @Transactional(readOnly = true)
    @Transactional
    public boolean isUserBanned(Long userId) {
        if (userId == null) return true; // (정책) ID 없는 접근은 차단
        String tempBanKey = TEMP_BAN_KEY_PREFIX + userId;

        try {
            // 1. 임시 밴(Redis) 확인
            if (redisTemplateForString.hasKey(tempBanKey)) {
                log.warn("[접속 확인] 사용자 {} 임시 밴 상태", userId);
                return true;
            }

        } catch (Exception e) {
            log.error("사용자 {} 임시 밴 확인 중 Redis 오류 발생", userId, e);
            // Redis 장애 시, DB만 확인 (Fail-Open에서 Fail-Partial로 변경)
        }

        // 2. 영구 밴(DB) 확인
        // (Redis 장애가 발생했거나, 임시 밴이 없을 경우 DB 확인)
        return userRepository.findById(userId)
                .map(user -> {
                    boolean isBanned = (user.getUserStatus() == UserStatus.BAN);
                    if (isBanned) {
                        log.warn("[접속 확인] 사용자 {} 영구 밴 상태", userId);
                    }
                    return isBanned;
                })
                .orElse(false); // (정책) DB에 유저 없으면 밴 아님
    }
}