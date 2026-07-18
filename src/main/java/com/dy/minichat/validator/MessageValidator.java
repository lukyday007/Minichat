package com.dy.minichat.validator;

import com.dy.minichat.dto.request.MessageRequestDTO;
import com.dy.minichat.dto.request.LastReadMessageRequestDTO;
import com.dy.minichat.entity.Chat;
import com.dy.minichat.entity.User;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MessageValidator {

    public void validateCreateMessage(MessageRequestDTO dto, long senderId, long chatId) {
        if (dto == null || dto.getContent() == null || dto.getContent().trim().isEmpty())
            throw new IllegalArgumentException("메시지 내용은 비어있을 수 없습니다.");
        if (senderId <= 0 || chatId <= 0)
            throw new IllegalArgumentException("유효하지 않은 사용자 ID 또는 채팅방 ID입니다.");
    }

    public void validateReadRequest(LastReadMessageRequestDTO dto, Long userId, Long chatId) {
        if (userId == null || chatId == null)
            throw new IllegalArgumentException("사용자 ID와 채팅방 ID는 필수값입니다.");
        if (dto == null || dto.getLastMessageId() == null)
            throw new IllegalArgumentException("읽음 처리할 메시지 ID 정보가 부족합니다.");
    }

    public void validateMessageListParams(Long chatId, Long userId, int page, int size) {
        if (chatId == null || userId == null)
            throw new IllegalArgumentException("채팅방 ID와 사용자 ID는 필수값입니다.");
        if (page < 0 || size <= 0)
            throw new IllegalArgumentException("페이지 번호는 0 이상, 사이즈는 1 이상이어야 합니다.");
    }

    public void validateSystemEntryMessage(Chat chat, List<User> users) {
        if (chat == null || chat.getId() == null)
            throw new IllegalArgumentException("올바른 채팅방 정보가 필요합니다.");
        if (users == null || users.isEmpty())
            throw new IllegalArgumentException("입장한 유저 목록이 비어있습니다.");
    }

    public void validateSystemLeaveMessage(Chat chat, User leavingUser) {
        if (chat == null || chat.getId() == null)
            throw new IllegalArgumentException("올바른 채팅방 정보가 필요합니다.");
        if (leavingUser == null || leavingUser.getId() == null)
            throw new IllegalArgumentException("퇴장 사용자 정보는 필수값입니다.");
    }
}