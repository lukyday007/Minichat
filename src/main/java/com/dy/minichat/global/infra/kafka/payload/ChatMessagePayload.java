package com.dy.minichat.global.infra.kafka.payload;

import com.dy.minichat.dto.message.TalkMessageDTO;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChatMessagePayload {
    private TalkMessageDTO talkMessage;
}