package com.dy.minichat.global.aspect;

import com.dy.minichat.service.UserBanService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    private final RedisScript<Long> rateLimitScript; // 2번에서 등록한 Bean
    private final UserBanService userBanService;     // 3번에서 생성한 Bean

    private static final String RATE_LIMIT_KEY_PREFIX = "rate_limit:user:";
    private static final long WINDOW_SIZE_MS = 60000; // 1분 (60,000 ms)
    private static final long MESSAGE_LIMIT = 100;    // 분당 100회

    /**
     * Pointcut: WebSocketHandler의 handleTextMessage 메서드를 대상
     */
    @Pointcut("execution(* com.dy.minichat.global.infra.websocket.WebSocketHandler.handleTextMessage(..))")
    public void webSocketMessageHandling() {}

    @Around("webSocketMessageHandling()")
    public Object checkRateLimit(ProceedingJoinPoint joinPoint) throws Throwable {

        Object[] args = joinPoint.getArgs();
        WebSocketSession session = (WebSocketSession) args[0];

        // 1. 세션에서 userId 추출 (원본 핸들러의 private 메서드 대신 속성 직접 접근)
        Optional<Long> userIdOpt = getUserIdFromSessionAttributes(session);

        if (userIdOpt.isEmpty()) {
            // userId가 없는 비정상 세션은 원본 메서드가 처리하도록 그대로 진행
            return joinPoint.proceed();
        }

        Long userId = userIdOpt.get();

        // [신규 로직] 속도 제한(Lua) 검사 *전*에 밴 상태부터 확인
        if (userBanService.isUserBanned(userId)) {
            log.warn("[AOP 차단] 이미 밴 상태(임시 또는 영구)인 사용자 {}의 메시지 전송 시도", userId);
            // 밴 상태 유저의 연결은 즉시 종료
            session.close(CloseStatus.POLICY_VIOLATION.withReason("Banned User"));
            // 원본 메서드 실행 중단
            return null;
        }

        String redisKey = RATE_LIMIT_KEY_PREFIX + userId;
        long currentTime = Instant.now().toEpochMilli();
        // ZADD의 member는 고유해야 함 (동일 timestamp 동시 요청 구분)
        String uniqueMember = currentTime + ":" + UUID.randomUUID().toString();

        try {
            // 2. Lua 스크립트 실행
            Long result = redisTemplateForString.execute(
                    rateLimitScript,
                    Collections.singletonList(redisKey),
                    String.valueOf(currentTime),
                    String.valueOf(WINDOW_SIZE_MS),
                    String.valueOf(MESSAGE_LIMIT),
                    uniqueMember
            );

            // 3. 결과 확인 (1: 한도 초과, 0: 한도 이내)
            if (result != null && result == 1) {
                // 한도 초과!
                log.warn("[RateLimit] 사용자 {} 밴 처리 ({}ms 동안 {}회 초과)",
                        userId, WINDOW_SIZE_MS, MESSAGE_LIMIT);

                // 4. 사용자 밴 처리
                userBanService.applyStrike(userId);

                // 5. 웹소켓 연결 강제 종료
                session.close(CloseStatus.POLICY_VIOLATION.withReason("Message rate limit exceeded."));

                // 6. 원본 handleTextMessage 메서드 실행 중단 (null 반환)
                return null;
            }

        } catch (Exception e) {
            // Redis 장애 시 요청을 막기보다 일단 통과시키는 것이 나을 수 있음 (Fail-Open)
            log.error("[RateLimit] Redis 스크립트 실행 오류. user: {}", userId, e);
        }

        // 7. 한도 이내: 원본 메서드(handleTextMessage) 실행
        return joinPoint.proceed();
    }

    /**
     * AOP Aspect에서 세션 속성에 직접 접근하여 userId를 가져옵니다.
     */
    private Optional<Long> getUserIdFromSessionAttributes(WebSocketSession session) {
        try {
            Map<String, Object> attributes = session.getAttributes();
            Object userIdObj = attributes.get("userId");

            if (userIdObj instanceof Long) {
                return Optional.of((Long) userIdObj);
            }
            return Optional.empty();

        } catch (Exception e) {
            log.error("AOP 세션 userId 추출 실패. Session ID: {}", session.getId(), e);
            return Optional.empty();
        }
    }
}