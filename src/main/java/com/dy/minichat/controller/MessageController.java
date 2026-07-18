package com.dy.minichat.controller;

import com.dy.minichat.dto.request.LastReadMessageRequestDTO;
import com.dy.minichat.dto.response.MessageResponseDTO;
import com.dy.minichat.global.model.BaseResponseBody;
import com.dy.minichat.service.MessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/minichat")
@RequiredArgsConstructor
public class MessageController {
    private final MessageService messageService;

    // == 메세지 목록 및 안 읽은 사람 수 반환 API == //
    @GetMapping("/chats/{chatId}/messages")
    public ResponseEntity<List<MessageResponseDTO>> getMessageListWithUnreadCounts (
            @PathVariable Long chatId,
            @AuthenticationPrincipal Long userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        List<MessageResponseDTO> data = messageService.getMessageListWithUnreadCounts(chatId, userId, page, size);
        return ResponseEntity.status(200).body(data);
    }

    // == 메세지 읽음 상태 업데이트 API == //
    @PutMapping("/chats/{chatId}/read")
    public ResponseEntity<BaseResponseBody> updateLastReadMessage(
            @PathVariable Long chatId,
            @AuthenticationPrincipal Long curUserId,
            @RequestBody LastReadMessageRequestDTO request
    ) {
        messageService.updateLastReadMessage(request, curUserId, chatId);
        return ResponseEntity.status(201).body(BaseResponseBody.of(201, "마지막으로 읽은 메세지 상태 업데이트 성공."));
    }
}