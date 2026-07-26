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
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    private final RedisScript<Long> rateLimitScript;
    private final UserBanService userBanService;

    private static final String RATE_LIMIT_KEY_PREFIX = "rate_limit:user:";
    private static final long WINDOW_SIZE_MS = 60000; // 1분 (60,000 ms)
    private static final long MESSAGE_LIMIT = 100;    // 분당 100회

    private final AtomicLong redisCircuitOpenUntil = new AtomicLong(0);
    private static final long CIRCUIT_OPEN_MS = 5000;

    /*
     * Pointcut: WebSocketHandler의 handleTextMessage 메서드를 대상
     */
    @Pointcut("execution(* com.dy.minichat.global.infra.websocket.WebSocketHandler.handleTextMessage(..))")
    public void webSocketMessageHandling() {}


    @Around("webSocketMessageHandling()")
    public Object checkRateLimit(ProceedingJoinPoint joinPoint) throws Throwable {

        Object[] args = joinPoint.getArgs();
        WebSocketSession session = (WebSocketSession) args[0];

        Optional<Long> userIdOpt = getUserIdFromSessionAttributes(session);

        if (userIdOpt.isEmpty()) {
            return joinPoint.proceed();
        }

        Long userId = userIdOpt.get();

        // 서킷 열림 → Redis 기반 검사 전체를 건너뛰고 통과 (Fail-Open)
        long now = System.currentTimeMillis();
        if (now < redisCircuitOpenUntil.get()) {
            return joinPoint.proceed();
        }

        String redisKey = RATE_LIMIT_KEY_PREFIX + userId;
        long currentTime = Instant.now().toEpochMilli();
        String uniqueMember = currentTime + ":" + UUID.randomUUID().toString();

        try {
            // 밴 상태 확인 (Redis 기반이므로 try 안에서 처리)
            if (userBanService.isUserBanned(userId)) {
                log.warn("[AOP 차단] 이미 밴 상태(임시 또는 영구)인 사용자 {}의 메시지 전송 시도", userId);
                session.close(CloseStatus.POLICY_VIOLATION.withReason("Banned User"));
                return null;
            }

            // Lua 스크립트 실행
            Long result = redisTemplateForString.execute(
                    rateLimitScript,
                    Collections.singletonList(redisKey),
                    String.valueOf(currentTime),
                    String.valueOf(WINDOW_SIZE_MS),
                    String.valueOf(MESSAGE_LIMIT),
                    uniqueMember
            );

            // 결과 확인 (1: 한도 초과, 0: 한도 이내)
            if (result != null && result == 1) {
                log.warn("[RateLimit] 사용자 {} 밴 처리 ({}ms 동안 {}회 초과)", userId, WINDOW_SIZE_MS, MESSAGE_LIMIT);
                userBanService.applyStrike(userId);
                session.close(CloseStatus.POLICY_VIOLATION.withReason("Message rate limit exceeded."));
                return null;
            }

        } catch (Exception e) {
            // Redis 장애 → 서킷 오픈 (5초간 Redis 호출 자체를 건너뜀)
            redisCircuitOpenUntil.set(System.currentTimeMillis() + CIRCUIT_OPEN_MS);
            log.error("[서킷 오픈] RateLimit Redis 실패 → {}ms간 Fail-Open. user: {}", CIRCUIT_OPEN_MS, userId, e);

            // 3회 밴을 제외한 다른 밴일 유저인 경우
            //      degradation : 장애 5초 창 동안 밴/속도제한 미적용
        }

        // 한도 이내 또는 서킷 오픈: 원본 메서드(handleTextMessage) 실행
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