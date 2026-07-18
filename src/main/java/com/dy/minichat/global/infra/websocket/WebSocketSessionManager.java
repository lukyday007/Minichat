package com.dy.minichat.global.infra.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
    stateful -> before & after state different
    ConcurrentHashMap -> asychronized proccess is neccessary!!
 */

@Component
public class WebSocketSessionManager {
    private final Map<Long, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public void addSession(Long userId, WebSocketSession session) {

        // == Spring의 WebSocketSession은 thread-safe X ==//
        // 두 개 이상의 스레드가 동시에 데이터를 쓰는(write) 행위 불가
        // 동시 전송 시 IllegalStateException(TEXT_PARTIAL_WRITING) 방지
        WebSocketSession safeSession = new ConcurrentWebSocketSessionDecorator(
                session,
                5000,      // send 대기 타임아웃(ms)
                512 * 1024 // 버퍼 크기 제한(bytes)
        );
        sessions.put(userId, safeSession);
    }


    public void removeSession (Long userId) {
        sessions.remove(userId);
    }

    public WebSocketSession getSession (Long userId) {
        return sessions.get(userId);
    }

    public Map<Long, WebSocketSession> getSessions() {
        return this.sessions;
    }
}