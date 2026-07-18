package com.dy.minichat.dto.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
public class UserChatResponseDTO {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long chatId;

    private String title;
    private String lastMessageContent;
    private LocalDateTime lastMessageTimestamp;
}
