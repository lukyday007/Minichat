package com.dy.minichat.config;

import jakarta.annotation.PostConstruct;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
@ConditionalOnProperty(prefix = "firebase", name = "enabled", havingValue = "true", matchIfMissing = false)
@Configuration
@RequiredArgsConstructor
public class FirebaseConfig {

    // yml에 정의된 파일 경로를 Resource 객체로 주입받음
    @Value("${firebase.secret-key-path}")
    private Resource serviceAccount;

    @PostConstruct
    public void initialize() {
        try {
            // FirebaseApp이 비어있을 때만 초기화
            if (FirebaseApp.getApps().isEmpty()) {
                // 주입받은 Resource 객체로부터 InputStream을 얻어옴
                InputStream serviceAccountStream = serviceAccount.getInputStream();

                FirebaseOptions options = FirebaseOptions.builder()
                        .setCredentials(GoogleCredentials.fromStream(serviceAccountStream))
                        .build();

                FirebaseApp.initializeApp(options);
                log.info("FirebaseApp initialized successfully.");
            }
        } catch (IOException e) {
            log.error("Failed to initialize FirebaseApp.", e);
            // 초기화 실패 시 애플리케이션을 시작하지 못하게 예외를 던지는 것도 좋은 방법입니다.
            throw new RuntimeException("Failed to initialize FirebaseApp.", e);
        }
    }
}