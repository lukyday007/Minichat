package com.dy.minichat.dto.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
@AllArgsConstructor
public class ChatResponseDTO {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long id;

    private String title;
    private LocalDateTime createdAt;
}