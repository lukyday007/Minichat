package com.dy.minichat.global.infra.websocket;

import com.dy.minichat.global.security.JwtTokenProvider;
import com.dy.minichat.service.UserBanService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketHandshakeInterceptor implements HandshakeInterceptor {

    private final JwtTokenProvider jwtTokenProvider;
    private final UserBanService userBanService; // 밴 유저는 연결 자체를 차단

    @Override
    public boolean beforeHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Map<String, Object> attributes
    ) throws Exception {

        // 1. URI에서 'token' 쿼리 파라미터 추출
        String token = extractTokenFromUri(request.getURI());
        // log.info("[Handshake] raw token from URI: '{}'", token);

        if (!StringUtils.hasText(token)) {
            token = extractTokenFromHeader(request.getHeaders());
            // log.info("[Handshake] raw token from Header: '{}'", token);
        }

        // 2. 토큰 유효성 검사
        if (StringUtils.hasText(token) && jwtTokenProvider.validateToken(token)) {
            // 3. 토큰에서 userId 추출
            Long userId = jwtTokenProvider.getUserIdFromToken(token);

            // 4. [AOP 연동] 밴 상태 확인 (AOP보다 먼저 차단)
            if (userBanService.isUserBanned(userId)) {
                log.warn("[Handshake] 밴 상태인 사용자 {}의 연결 시도 차단", userId);
                return false; // 핸드셰이크 거부
            }

            // 5. [핵심] userId를 세션 속성(attributes)에 저장
            // -> 이 userId를 RateLimitAspect가 사용하게 됩니다.
            attributes.put("userId", userId);

            // log.info("[Handshake] 인증 성공. userId: {}", userId);
            return true; // 핸드셰이크 승인
        }

        log.warn("[Handshake] 인증 실패. 유효하지 않은 토큰.");
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        return false; // 핸드셰이크 거부
    }

    @Override
    public void afterHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Exception exception
    ) {
        // (필요 없음)
    }

    private String extractTokenFromHeader(HttpHeaders headers) {
        String bearer = headers.getFirst(HttpHeaders.AUTHORIZATION);
        if (StringUtils.hasText(bearer) && bearer.startsWith("Bearer ")) {
            return bearer.substring(7);
        }
        return null;
    }

    private String extractTokenFromUri(URI uri) {
        return UriComponentsBuilder.fromUri(uri)
                .build()
                .getQueryParams()
                .getFirst("token");
    }
}