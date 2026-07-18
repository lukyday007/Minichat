package com.dy.minichat.validator;

import com.dy.minichat.dto.request.ChatRequestDTO;
import com.dy.minichat.dto.request.InviteRequestDTO;
import com.dy.minichat.entity.User;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class ChatValidator {

    // createChat 진입 시 형태 검증
    public void validateCreateChatRequest(Long creatorId, ChatRequestDTO dto) {
        if (creatorId == null) throw new IllegalArgumentException("생성자 ID는 필수값입니다.");
        if (dto == null) throw new IllegalArgumentException("요청 정보가 비어있습니다.");
        if (dto.getUserIds() == null) throw new IllegalArgumentException("초대할 유저 목록은 필수값입니다.");
    }

    // inviteUsersToChat 진입 시 형태 검증
    public void validateInviteRequest(Long inviterId, Long chatId, InviteRequestDTO dto) {
        if (inviterId == null || chatId == null)
            throw new IllegalArgumentException("초대자 ID와 채팅방 ID는 필수값입니다.");
        if (dto == null) throw new IllegalArgumentException("요청 정보가 비어있습니다.");
    }

    // 조회 후 의미 검증
    public void validateUsersFound(List<User> users, Set<Long> requestedIds) {
        if (users.isEmpty())
            throw new IllegalArgumentException("유효한 채팅 참여자가 없습니다. 요청 IDs: " + requestedIds);
    }

    public void validateCreatorIncluded(List<User> users, Long creatorId) {
        boolean creatorExists = users.stream().anyMatch(u -> u.getId().equals(creatorId));
        if (!creatorExists)
            throw new IllegalArgumentException("방 생성자 정보가 올바르지 않거나 존재하지 않는 회원입니다. creatorId: " + creatorId);
    }
}