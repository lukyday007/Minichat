package com.dy.minichat.service;

import com.dy.minichat.dto.message.TalkMessageDTO;
import com.dy.minichat.dto.response.ChatResponseDTO;
import com.dy.minichat.dto.response.MessageResponseDTO;
import com.dy.minichat.global.id.ChatIdGenerator;
import com.dy.minichat.global.id.UserChatIdGenerator;
import com.dy.minichat.dto.request.ChatRequestDTO;
import com.dy.minichat.dto.request.InviteRequestDTO;
import com.dy.minichat.dto.response.UserChatResponseDTO;
import com.dy.minichat.entity.*;
import com.dy.minichat.global.infra.kafka.payload.ChatMessagePayload;
import com.dy.minichat.global.infra.kafka.producer.ChatMessageProducer;
import com.dy.minichat.repository.ChatRepository;
import com.dy.minichat.repository.UserChatRepository;
import com.dy.minichat.repository.UserRepository;
import com.dy.minichat.validator.ChatValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {
    private final MessageService messageService;

    private final ChatRepository chatRepository;
    private final UserRepository userRepository;
    private final UserChatRepository userChatRepository;

    private final ChatValidator chatValidator;

    private final ChatIdGenerator chatIdGenerator;
    private final UserChatIdGenerator userChatIdGenerator;

    private final ChatMessageProducer chatMessageProducer;

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    private final String serverIdentifier;

    private static final Duration USER_STATE_TTL = Duration.ofHours(24);
    private static final Duration CHAT_MEMBERS_TTL = Duration.ofHours(24);

    private final AtomicLong redisCircuitOpenUntil = new AtomicLong(0);
    private static final long CIRCUIT_OPEN_MS = 5000;


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
    public ChatResponseDTO createChat(Long creatorId, ChatRequestDTO dto) {
        chatValidator.validateCreateChatRequest(creatorId, dto);

        Set<Long> allUserIds = new HashSet<>(dto.getUserIds());
        allUserIds.add(creatorId);

        // DB에서 유저 실제 존재 여부 검증
        List<User> users = userRepository.findAllById(allUserIds); // snowflake아이디 설정 사용 -> repository Long => String 전환
        chatValidator.validateUsersFound(users, allUserIds);

        // 방 생성자(creatorId)가 유저 조회 결과에 실제로 포함되어 있는지 무결성 검증
        chatValidator.validateCreatorIncluded(users, creatorId);

        // 채팅방 이름이 공백이거나 null일 때의 기본값 대입 (DB 오염 방지)
        String chatTitle = (dto.getTitle() != null && !dto.getTitle().trim().isEmpty())
                ? dto.getTitle()
                : "이름 없는 채팅방";

        // 채팅방 저장
        Chat chat = new Chat();
        chat.setId(chatIdGenerator.generate());
        chat.setTitle(chatTitle);
        chatRepository.save(chat);

        associateUsersWithChat(chat, users);

        // 시스템 메시지 생성 및 DB 저장
        MessageResponseDTO systemMessage = messageService.createSystemEntryMessage(chat, users);

        // DB 저장 후 카프카 메세지 발행
        publishSystemMessageToKafka(chat.getId(), systemMessage);

        return ChatResponseDTO.builder()
                .id(chat.getId())
                .title(chat.getTitle())
                // @CreationTimestamp 작동하지 않고 null 반환 할 경우 대비
                .createdAt(chat.getCreatedAt() != null ? chat.getCreatedAt() : LocalDateTime.now())
                .build();
    }


    // == 채팅방에 유저 초대 API == //
    @Transactional
    public Long inviteUsersToChat(Long inviterId, Long chatId, InviteRequestDTO dto) {
        chatValidator.validateInviteRequest(inviterId, chatId, dto);

        if (dto.getUserIds() == null || dto.getUserIds().isEmpty()) {
            log.warn("[유저 초대 중단] 초대할 유저 ID 목록이 비어있습니다. chatId: {}", chatId);
            return chatId; // 초대할 대상이 없으므로 더이상 진행하지 않고 조기 리턴
        }

        // 초대자 권한 검증 (본인이 참여 중인 방인지 먼저 확인)
        userChatRepository.findByUserIdAndChatIdAndIsDeletedFalse(inviterId, chatId)
                .orElseThrow(() -> {
                    log.warn("[유저 초대 권한 없음] 금지된 요청. inviterId: {}, chatId: {}", inviterId, chatId);
                    return new AccessDeniedException("초대 권한이 없습니다."); // 403 Forbidden 유발
                });

        // 대상 채팅방 존재 여부 검증
        Chat existingChat = getChatOrThrow(chatId);

        // 기존 참여 유저 목록 조회
        List<Long> existingMemberIds = userChatRepository.findUserIdsByChatId(chatId);

        // DB 조회 및 검증 (이미 참여 중인 유저 제외 & 실제 DB에 존재하는 유저만 필터링)
        List<User> newInvitedUsers = userRepository.findAllById(dto.getUserIds()).stream()
                .filter(user -> !existingMemberIds.contains(user.getId()))
                .collect(Collectors.toList());

        // 유효하게 초대할 유저가 한 명도 없는 경우 (이미 다 참여 중이거나 유효하지 않은 ID일 때)
        if (newInvitedUsers.isEmpty()) {
            log.info("[유저 초대 중단] 새로 초대할 유저가 없거나 이미 모두 참여 중입니다. chatId: {}", chatId);
            return existingChat.getId();
        }

        // 유저와 채팅방 연관 관계 매핑
        associateUsersWithChat(existingChat, newInvitedUsers);

        // 시스템 메시지 생성 및 DB 저장
        MessageResponseDTO systemMessage = messageService.createSystemEntryMessage(existingChat, newInvitedUsers);

        // DB 저장 후 카프카 메세지 발행
        publishSystemMessageToKafka(existingChat.getId(), systemMessage);

        return existingChat.getId();
    }


    private void associateUsersWithChat(Chat chat, List<User> users) {
        List<UserChat> userChats = users.stream()
                .map(user -> {
                    UserChat userChat = new UserChat();
                    userChat.setId(userChatIdGenerator.generate());
                    userChat.setUser(user);
                    userChat.setChat(chat);
                    return userChat;
                })
                .toList();
        // .save()에서 병목 발생 -> .saveAll()로 전환
        userChatRepository.saveAll(userChats);
    }


    // 방 들어갔을 때의 Redis - 현재 이 채팅방에 접속해서 활성화
    public void enterChatRoom(Long userId, Long chatId) {

        if (userId == null || chatId == null) {
            log.error("[입장 실패] 필수 파라미터 누락. userId: {}, chatId: {}", userId, chatId);
            throw new IllegalArgumentException("사용자 ID와 채팅방 ID는 필수값입니다.");
        }

        boolean isMember = userChatRepository.existsByUserIdAndChatIdAndIsDeletedFalse(userId, chatId);
        if (!isMember) {
            log.warn("[입장 실패] 참여 권한이 없는 채팅방 입장 시도 방지. userId: {}, chatId: {}", userId, chatId);
            throw new AccessDeniedException("해당 채팅방에 참여 권한이 없는 사용자입니다.");
        }

        String userKey = "userId:" + userId + ":state";
        String userIdStr = String.valueOf(userId);

        // 이전 방이 있다면 퇴장 처리
        String oldChatIdStr = (String) redisTemplateForString.opsForHash().get(userKey, "chatId");
        if (oldChatIdStr != null) {
            redisTemplateForString.opsForSet().remove("chatId:" + oldChatIdStr + ":userId", userIdStr);
        }

        // 새로운 방 입장 처리
        String newChatKey = "chatId:" + chatId + ":userId";
        redisTemplateForString.opsForSet().add(newChatKey, userIdStr);
        redisTemplateForString.expire(newChatKey, CHAT_MEMBERS_TTL);

        // 분산 환경 인프라 식별자(serverIdentifier) 누락 대비 보호
        String activeServerId = (serverIdentifier != null) ? serverIdentifier : "UNKNOWN_SERVER";

        // 사용자의 현재 참여 채팅방 및 서버 정보 업데이트
        Map<String, String> userState = Map.of(
                "chatId", String.valueOf(chatId),
                "serverId", activeServerId,
                "lastActive", LocalDateTime.now().toString()
        );
        redisTemplateForString.opsForHash().putAll(userKey, userState);
        redisTemplateForString.expire(userKey, USER_STATE_TTL);
        log.info("[입장] user : {} -> chat : {} 상태 저장 완료 (Server: {})", userId, chatId, activeServerId);
    }

    // == 채팅방 목록 반환 API == //
    // - 디비레벨에서는 replica를 사용해서 scale out
    // - jpa레벨에서는 스냅샷안찍음, 트랜잭션끝날때 더티체킹안함, ... 소소한 최적화
    @Transactional(readOnly = true)
    public List<UserChatResponseDTO> getChatRoomsList(Long userId) {

        // 필수 파라미터 null 검증
        if (userId == null) {
            log.error("[채팅방 목록 조회 실패] 사용자 ID가 null입니다.");
            throw new IllegalArgumentException("사용자 ID는 필수값입니다.");
        }

        List<UserChat> chatRooms = userChatRepository.findAllByUserIdOrderByLastMessageTimestampDesc(userId);

        // message 추가 scale-out -> 샤딩
        // List<Long> messageIds = chatRooms.map .... message Id....
        // List<Message> messages = ...

        if (chatRooms == null || chatRooms.isEmpty()) {
            log.warn("[채팅방 목록 조회] 참여 중인 채팅방이 존재하지 않습니다. userId: {}", userId);
            return Collections.emptyList();
        }

        return chatRooms.stream()
                .map(userChat -> {
                    // 연관 Chat 엔티티 누락 무결성 검증 (NPE 방지)
                    Chat chat = userChat.getChat();
                    if (chat == null) {
                        log.error("[채팅방 목록 무결성 오류] UserChat 엔티티에 연관된 Chat 정보가 null입니다. userChatId: {}", userChat.getId());
                        return null; // 하단 필터링을 통해 무결성이 깨진 데이터는 목록에서 제외
                    }

                    Message lastWrittenMessage = userChat.getLastWrittenMessage();
                    String content;
                    LocalDateTime timestamp;

                    // 마지막 메세지 확인 및 안전 처리
                    if (lastWrittenMessage != null) {
                        content = lastWrittenMessage.getContent();
                        timestamp = userChat.getLastMessageTimestamp();

                        // 마지막 메시지 시각이 간혹 누락되었을 때를 대비한 Fallback
                        if (timestamp == null) {
                            timestamp = userChat.getCreatedAt();
                        }
                    } else {
                        content = "아직 작성된 메시지가 없습니다.";
                        // @CreationTimestamp 미작동을 대비한 최후의 보루
                        timestamp = userChat.getCreatedAt() != null ? userChat.getCreatedAt() : LocalDateTime.now();
                    }

                    return new UserChatResponseDTO(
                            chat.getId(),
                            chat.getTitle() != null ? chat.getTitle() : "이름 없는 채팅방",
                            content,
                            timestamp
                    );
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }


    @Transactional
    public void leaveChatRoom(Long userId, Long chatId) {

        if (userId == null || chatId == null) {
            log.error("[퇴장 실패] 필수 파라미터 누락. userId: {}, chatId: {}", userId, chatId);
            throw new IllegalArgumentException("사용자 ID와 채팅방 ID는 필수값입니다.");
        }

        // UserChat 정보 먼저 조회 (삭제하기 전 사용자 이름 알기)
        UserChat userChat = userChatRepository.findByUserIdAndChatIdAndIsDeletedFalse(userId, chatId)
                .orElseThrow(() -> {
                    log.warn("[퇴장 실패] 존재하지 않거나 이미 삭제된 참여 정보. userId: {}, chatId: {}", userId, chatId);
                    return new IllegalArgumentException("채팅방 참여 정보를 찾을 수 없습니다.");
                });
        userChat.setDeleted(true);

        // Redis 작업
        String userIdStr = String.valueOf(userId);
        String chatIdStr = String.valueOf(chatId);
        String chatUsersKey = "chatId:" + chatIdStr + ":userId";

        redisTemplateForString.opsForSet().remove(chatUsersKey, userIdStr);

        String userKey = "userId:" + userId + ":state";
        redisTemplateForString.delete(userKey);

        User leavingUser = userChat.getUser();
        Chat chat = userChat.getChat();

        MessageResponseDTO systemMessage = messageService.createSystemLeaveMessage(chat, leavingUser);

        // 카프카 메시지 발행
        publishSystemMessageToKafka(chat.getId(), systemMessage);
    }

    private void publishSystemMessageToKafka(Long chatId, MessageResponseDTO messageDto) {
        TalkMessageDTO talkMessageDto = TalkMessageDTO.builder()
                .chatId(chatId)
                .content(messageDto.getContent())
                .senderId(messageDto.getSenderId())     // 0L
                .type(messageDto.getType())             // SYSTEM_ENTRY 또는 SYSTEM_LEAVE
                .timestamp(java.time.Instant.now())
                .build();

        ChatMessagePayload kafkaEvent = ChatMessagePayload.builder()
                .talkMessage(talkMessageDto)
                .build();

        chatMessageProducer.send(kafkaEvent);
    }


    public Set<Long> getUsersInChat(Long chatId) {
        long now = System.currentTimeMillis();

        // 서킷 열림 → Redis 건너뛰고 DB 직행
        if (now < redisCircuitOpenUntil.get()) {
            return userChatRepository.findUserIdsByChatIdWithSet(chatId);
        }

        try {
            Set<String> ids = redisTemplateForString.opsForSet()
                    .members("chatId:" + chatId + ":userId");
            if (ids != null && !ids.isEmpty()) {
                return ids.stream().map(Long::valueOf).collect(Collectors.toSet());
            }
        } catch (Exception e) {
            redisCircuitOpenUntil.set(System.currentTimeMillis() + CIRCUIT_OPEN_MS);
            log.error("[서킷 오픈] Redis 방 멤버 조회 실패 → {}ms간 DB 직행. chatId={}",
                    CIRCUIT_OPEN_MS, chatId, e);
        }

        return userChatRepository.findUserIdsByChatIdWithSet(chatId);
    }

    // ========

    private Chat getChatOrThrow(Long chatId) {
        return chatRepository.findById(chatId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방을 찾을 수 없습니다. id=" + chatId));
    }
}