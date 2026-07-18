package com.dy.minichat.dto.response;

import com.dy.minichat.entity.MessageType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
@AllArgsConstructor
public class MessageResponseDTO {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long id;

    @JsonSerialize(using = ToStringSerializer.class)
    private Long senderId;

    private String senderName;

    @JsonSerialize(using = ToStringSerializer.class)
    private Long chatId;

    private String content;
    private MessageType type;
    private int unReadCnt;
}