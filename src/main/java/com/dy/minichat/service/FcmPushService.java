package com.dy.minichat.service;

import com.dy.minichat.dto.message.TalkMessageDTO;
import com.dy.minichat.repository.UserRepository;
import com.dy.minichat.entity.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class FcmPushService {
    private final FcmClient fcmClient; // Firebase SDK 래핑 클래스
    private final FcmTokenService fcmTokenService; // 유저의 FCM 토큰 조회용
    private final UserRepository userRepository;

    @Value("${firebase.enabled:false}")
    private boolean firebaseEnabled;

    public void sendPushNotification(Long recipientId, TalkMessageDTO messageDTO) {

        if (!firebaseEnabled) return;   // 로드테스트에선 아무것도 안 함

        String token = fcmTokenService.getTokenByUserId(recipientId);

        String senderName = userRepository.findById(messageDTO.getSenderId())
                .map(User::getName) // User 객체의 getName() 메서드 사용
                .orElse("알 수 없는 사용자");

        String title = senderName;
        String body = messageDTO.getContent();
        Map<String, String> data = Map.of(
                "type", "NEW_MESSAGE",
                "chatId", String.valueOf(messageDTO.getChatId()),
                "senderId", String.valueOf(messageDTO.getSenderId()),
                "senderName", senderName,
                "content", messageDTO.getContent(),
                "sentAt", messageDTO.getTimestamp().toString()
        );

        fcmClient.sendMessage(token, title, body, data);
    }
}