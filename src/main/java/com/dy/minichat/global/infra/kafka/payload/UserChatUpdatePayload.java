package com.dy.minichat.global.infra.kafka.payload;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserChatUpdatePayload {
    private Long chatId;
    private Long lastMessageId;
    private LocalDateTime timestamp;
}