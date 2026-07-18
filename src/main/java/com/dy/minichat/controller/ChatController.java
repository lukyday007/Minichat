package com.dy.minichat.controller;

import com.dy.minichat.dto.request.ChatRequestDTO;
import com.dy.minichat.dto.request.InviteRequestDTO;
import com.dy.minichat.dto.response.ChatResponseDTO;
import com.dy.minichat.dto.response.UserChatResponseDTO;
import com.dy.minichat.global.model.BaseResponseBody;
import com.dy.minichat.service.ChatService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/minichat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatService chatService;

    // == 채팅방 생성 API == //
    @PostMapping("/chat")
    public ResponseEntity<ChatResponseDTO> createChat(
            @AuthenticationPrincipal Long creatorId,
            @RequestBody ChatRequestDTO request
    ) {
        return ResponseEntity.status(201).body(chatService.createChat(creatorId, request));
    }

    // == 특정 생성방에 입장하는 API = //
    @PostMapping("/chat/{chatId}/enter")
    public ResponseEntity<BaseResponseBody> enterChatRoom(
            @PathVariable Long chatId,
            @AuthenticationPrincipal Long userId
    ) {
        chatService.enterChatRoom(userId, chatId);
        return ResponseEntity.status(201).body(BaseResponseBody.of(201, "채팅방 입장"));
    }

    // == 채팅방 나가기 API == //
    @DeleteMapping("/chats/{chatId}/leave")
    public ResponseEntity<BaseResponseBody> leaveChat(
            @PathVariable Long chatId,
            // 인증된 사용자 정보
            @AuthenticationPrincipal Long userId
    ) {
        chatService.leaveChatRoom(userId, chatId);
        return ResponseEntity.status(200).body(BaseResponseBody.of(200, "채팅방을 나갔습니다."));
    }

    // == 채팅방 목록 반환 API == //
    @GetMapping("/chats")
    public ResponseEntity<List<UserChatResponseDTO>> getChatRoomsList(
            @AuthenticationPrincipal Long userId   // 추후 인증 로직 추가
    ) {
        // 유저 인증 후 해당 유저가 참가한 채팅방 목록 조회
        List<UserChatResponseDTO> data = chatService.getChatRoomsList(userId);
        return ResponseEntity.status(200).body(data);
    }

    // == 채팅방에 유저 초대 API == //
    @PostMapping("/chats/{chatId}/invite")
    public ResponseEntity<BaseResponseBody> inviteUsersToChat(
            @AuthenticationPrincipal Long inviterId,
            @PathVariable Long chatId,
            @RequestBody InviteRequestDTO request
    ) {
        chatService.inviteUsersToChat(inviterId, chatId, request);
        return ResponseEntity.status(201).body(BaseResponseBody.of(201, "채팅방에 유저를 초대했습니다."));
    }
}