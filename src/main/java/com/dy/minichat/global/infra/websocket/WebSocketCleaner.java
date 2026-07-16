package com.dy.minichat.global.infra.websocket;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketCleaner {

    private final WebSocketSessionManager sessionManager;

    @PreDestroy
    public void clean(){
        // for
        // 붙어있는 모든 웹소켓을 하나씩 돌면서 .close();
        // Thread.sleep 적당히 걸면서
        // 1초에 1000개씩 끊는다.
        // 10만개의 소켓을 끊기위해서 100초 사용됨 -> graceful shutdown

        log.info("Graceful Shutdown: 활성 웹소켓 세션의 순차적 종료를 시작합니다.");
        List<WebSocketSession> sessionsToClose = new ArrayList<>(sessionManager.getSessions().values());
        int totalCnt = sessionsToClose.size();

        if (totalCnt == 0) {
            log.info("종료할 활성 웹소켓 세션이 없습니다.");
            return;
        }

        int batchSize = 1000;
        int closedCnt = 0;

        for (WebSocketSession session : sessionsToClose) {
            if (session.isOpen()) {
                try {
                    // 서버가 종료됨을 명시적으로 알리며 세션을 닫습니다.
                    session.close(CloseStatus.GOING_AWAY.withReason("Server is shutting down."));
                    closedCnt++;
                } catch (IOException e) {
                    log.warn("세션 종료 중 I/O 오류 발생. Session ID: {}", session.getId(), e);
                }
            }

            if (closedCnt > 0 && closedCnt % batchSize == 0 && closedCnt < totalCnt) {
                log.info("{} / {} 개의 세션 종료 완료. 다음 배치를 위해 1초 대기합니다.", closedCnt, totalCnt);
                try {
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    log.warn("웹소켓 종료 대기 중 스레드가 중단되었습니다.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}