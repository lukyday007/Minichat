package com.dy.minichat.service;

import com.dy.minichat.global.id.ChatIdGenerator;
import com.dy.minichat.global.id.UserChatIdGenerator;
import com.dy.minichat.dto.request.ChatRequestDTO;
import com.dy.minichat.dto.request.InviteRequestDTO;
import com.dy.minichat.dto.response.UserChatResponseDTO;
import com.dy.minichat.entity.*;
import com.dy.minichat.repository.ChatRepository;
import com.dy.minichat.repository.UserChatRepository;
import com.dy.minichat.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {
    private final MessageService messageService;

    private final ChatRepository chatRepository;
    private final UserRepository userRepository;
    private final UserChatRepository userChatRepository;

    private final ChatIdGenerator chatIdGenerator;
    private final UserChatIdGenerator userChatIdGenerator;

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    private final String serverIdentifier;

    /*
        // K: userId, V: 현재 입장해 있는 roomId
        // key: WebSocket 세션, value: (현재 참여한) 채팅방 ID  (원본 코드: Long 단일 값)
        private final Map<Long, Long> userToChat = new ConcurrentHashMap<>();

        // K: roomId, V: 해당 방에 있는 userId Set
        // key: 채팅방 ID, value: 해당 채팅방에 연결된 WebSocket 세션들의 집합
        private final Map<Long, Set<Long>> chatToUsers = new ConcurrentHashMap<>();
    */

    // == 채팅방 API == //
    @Transactional
    public Chat createChat(Long creatorId, ChatRequestDTO dto) {
        List<Long> userIds = dto.getUserIds();
        Set<Long> allUserIds = new HashSet<>(userIds);
        allUserIds.add(creatorId);

        // 유저 수에 따라 채팅방 상태 결정
        ChatStatus status = userIds.size() > 2 ? ChatStatus.GROUP : ChatStatus.DIRECT;

        // 채팅방 저장
        Chat chat = new Chat();
        chat.setId(chatIdGenerator.generate());
        chat.setTitle(dto.getTitle());
        chat.setStatus(status);
        chatRepository.save(chat);

        // snowflake아이디 설정 사용 -> repository Long => String 전환
        List<User> users = userRepository.findAllById(allUserIds);

        associateUsersWithChat(chat, users);

        // 시스템 메시지 생성 및 DB 저장
        Message systemMessage = messageService.createSystemEntryMessage(chat, users);

        // [수정] 직접 호출 대신 이벤트 발행 -> Kafka로 수정
        eventPublisher.publishEvent(new SystemMessageEvent(chat.getId(), systemMessage));

        return chat;
    }

    @Transactional
    public Chat createChat(ChatRequestDTO dto) {
        List<Long> userIds = dto.getUserIds();

        ChatStatus status = userIds.size() > 2 ? ChatStatus.GROUP : ChatStatus.DIRECT;

        Chat chat = new Chat();
        chat.setId(chatIdGenerator.generate());
        chat.setTitle(dto.getTitle());
        chat.setStatus(status);
        chatRepository.save(chat);

        List<User> users = userRepository.findAllById(userIds);

        associateUsersWithChat(chat, users);

        Message systemMessage = messageService.createSystemEntryMessage(chat, users);
        eventPublisher.publishEvent(new SystemMessageEvent(chat.getId(), systemMessage));

        return chat;
    }


    // == 채팅방에 유저 초대 API == //
    @Transactional
    public Long inviteUsersToChat(Long inviterId, Long chatId, InviteRequestDTO dto) {
        User inviter = userChatRepository.findByUserIdAndChatIdAndIsDeletedFalse(inviterId, chatId)
                .orElseThrow(() -> new AccessDeniedException("초대 권한이 없습니다.")) // 403 Forbidden 유발
                .getUser();

        Chat existingChat = chatRepository.findById(chatId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방을 찾을 수 없습니다."));

        List<Long> existingMemberIds = userChatRepository.findUserIdsByChatId(chatId);
        List<User> newInvitedUsers = userRepository.findAllById(dto.getUserIds()).stream()
                .filter(user -> !existingMemberIds.contains(user.getId()))
                .collect(Collectors.toList());

        if (newInvitedUsers.isEmpty()) {
            return existingChat.getId();
        }

        long totalMemberCount = existingMemberIds.size() + newInvitedUsers.size();

        // 조건에 따라 적절한 헬퍼 메서드를 호출
        if (existingChat.getStatus() == ChatStatus.DIRECT && totalMemberCount > 2) {
            return upgradeDirectChatToGroup(existingMemberIds, newInvitedUsers);
        } else {
            addMembersToGroupChat(inviter, existingChat, newInvitedUsers);
            return existingChat.getId();
        }
    }

    private Long upgradeDirectChatToGroup(List<Long> existingMemberIds, List<User> newInvitedUsers) {
        // 전체 멤버 ID 목록 생성
        existingMemberIds.addAll(newInvitedUsers.stream().map(User::getId).toList());
        List<Long> allMemberIds = existingMemberIds;

        // 새로운 그룹 채팅방의 이름 생성
        List<User> allUsers = userRepository.findAllById(allMemberIds);
        String newTitle = allUsers.stream()
                .map(User::getName)
                .collect(Collectors.joining(", "));

        // createChat DTO 준비 및 새 채팅방 생성
        ChatRequestDTO newChatRequest = new ChatRequestDTO(newTitle, allMemberIds);
        Chat newChat = this.createChat(newChatRequest);

        return newChat.getId();
    }

    private void addMembersToGroupChat(User inviter, Chat chat, List<User> newInvitedUsers) {

        associateUsersWithChat(chat, newInvitedUsers);

        // 시스템 메시지 생성 및 이벤트 발행
        String invitedUserNames = newInvitedUsers.stream()
                .map(User::getName)
                .collect(Collectors.joining(", "));

        Message systemMessage = messageService.createSystemEntryMessage(chat, newInvitedUsers);

        eventPublisher.publishEvent(new SystemMessageEvent(chat.getId(), systemMessage));
    }

    private void associateUsersWithChat(Chat chat, List<User> users) {
        for (User user : users) {
            UserChat userChat = new UserChat();
            userChat.setId(userChatIdGenerator.generate());
            userChat.setUser(user);
            userChat.setChat(chat);
            userChatRepository.save(userChat);
        }
    }


    // 방 들어갔을 때의 Redis - 현재 이 채팅방에 접속해서 활성화
    public void enterChatRoom(Long userId, Long chatId) {

        String userKey = "userId:" + userId + ":state";
        String userIdStr = String.valueOf(userId);

        //  이전 방이 있다면 퇴장 처리
        //      사용자의 현재 chatId를 Redis에서 조회
        String oldChatIdStr = (String) redisTemplateForString.opsForHash().get(userKey, "chatId");
        if (oldChatIdStr != null)
            redisTemplateForString.opsForSet().remove("chatId:" + oldChatIdStr + ":userId", userIdStr);

        //  새로운 방 입장 처리
        String newChatKey = "chatId:" + chatId + ":userId";
        //      새로운 방의 유저 Set에 현재 유저를 추가
        redisTemplateForString.opsForSet().add(newChatKey, userIdStr);

        //  사용자의 현재 참여 채팅방 및 서버 정보 업데이트
        Map<String, String> userState = Map.of(
                "chatId", String.valueOf(chatId),
                "serverId", serverIdentifier,
                "lastActive", LocalDateTime.now().toString()
        );
        redisTemplateForString.opsForHash().putAll(userKey, userState);
        log.info("[입장] user : {} -> chat : {} 상태 저장 완료", userId, chatId);

        /*
            나중에 redis TTL 설정하기!
        */
    }


    // == 채팅방 목록 반환 API == //
    // @Transactional(readOnly = true)
    // read only true
    // - 디비레벨에서는 replica를 사용해서 scale out
    // - jpa레벨에서는 스냅샷안찍음, 트랜잭션끝날때 더티체킹안함, ... 소소한 최적화
    public List<UserChatResponseDTO> getChatRoomsList(Long userId) {
        List<UserChat> chatRooms = userChatRepository.findAllByUserIdOrderByLastMessageTimestampDesc(userId);

        // message 가 샤딩 됐을 때
        // List<Long> messageIds = chatRooms.map .... message Id....
        // List<Message> messages = ...

        return chatRooms.stream()
                .map(userChat -> {
                    Chat chat = userChat.getChat();
                    Message lastWrittenMessage = userChat.getLastWrittenMessage();

                    String content;
                    LocalDateTime timestamp;

                    // 마지막 메세지 확인
                    if (lastWrittenMessage != null) {
                        content = lastWrittenMessage.getContent();
                        timestamp = userChat.getLastMessageTimestamp();
                    } else {
                        content = "아직 작성된 메시지가 없습니다.";
                        timestamp = userChat.getCreatedAt();
                    }

                    return new UserChatResponseDTO(
                            chat.getId(),
                            chat.getStatus(),
                            chat.getTitle(),
                            content,
                            timestamp
                    );
                })
                .collect(Collectors.toList());
    }


    @Transactional
    public void leaveChatRoom(Long userId, Long chatId) {

        // UserChat 정보 먼저 조회 (삭제하기 전 사용자 이름 알기)
        UserChat userChat = userChatRepository.findByUserIdAndChatIdAndIsDeletedFalse(userId, chatId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방 참여 정보를 찾을 수 없습니다."));
        userChat.setDeleted(true);

        String userKey = "userId:" + userId + ":state";
        String userIdStr = String.valueOf(userId);

        // 사용자가 마지막으로 있었던 채팅방 정보 조회
        String lastChatIdStr = String.valueOf(chatId);

        if (lastChatIdStr != null) {
            String chatUsersKey = "chatId:" + lastChatIdStr + ":userId";
            // 해당 채팅방의 유저 목록(Set)에서 사용자 제거
            redisTemplateForString.opsForSet().remove(chatUsersKey, userIdStr);
        }

        // 2. 사용자의 세션 정보(Hash)를 완전히 삭제
        redisTemplateForString.delete(userKey);

        // WebSocketHandler 통해 실시간 방송 실행
        // webSocketHandler.broadcastSystemMessage(chatId, systemMessage);

        User leavingUser = userChat.getUser();
        Chat chat = userChat.getChat();
        Message systemMessage = messageService.createSystemLeaveMessage(chat, leavingUser);

        // [수정] 직접 호출 대신 이벤트 발행 -> Kafka로 수정
        eventPublisher.publishEvent(new SystemMessageEvent(chatId, systemMessage));
    }


    public Set<Long> getUsersInChat(Long chatId) {
        Set<String> userIdsStr = redisTemplateForString.opsForSet().members("chatId:" + chatId + ":userId");

        if (userIdsStr == null || userIdsStr.isEmpty())
            return Collections.emptySet();

        return userIdsStr.stream()
                .map(Long::valueOf)
                .collect(Collectors.toSet());
    }
}