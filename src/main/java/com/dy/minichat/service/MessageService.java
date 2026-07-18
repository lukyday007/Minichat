package com.dy.minichat.service;

// import com.dy.minichat.component.TaskManager;
import com.dy.minichat.global.id.MessageIdGenerator;
import com.dy.minichat.dto.request.LastReadMessageRequestDTO;
import com.dy.minichat.dto.request.MessageRequestDTO;
import com.dy.minichat.dto.response.MessageResponseDTO;
import com.dy.minichat.entity.*;
import com.dy.minichat.repository.ChatRepository;
import com.dy.minichat.repository.MessageRepository;
import com.dy.minichat.repository.UserChatRepository;
import com.dy.minichat.repository.UserRepository;
import com.dy.minichat.validator.MessageValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageService {

    private final UserChatUpdateService userChatUpdateService;

    private final ChatRepository chatRepository;
    private final MessageRepository messageRepository;
    private final UserRepository userRepository;
    private final UserChatRepository userChatRepository;

    private final MessageValidator messageValidator;

    private final MessageIdGenerator messageIdGenerator;

    @Qualifier("redisTemplateForString")
    private final RedisTemplate<String, String> redisTemplateForString;
    private final RedisScript<Long> lastReadUpdateScript;


    // == 메세지 API == //
    @Transactional
    public void createMessage(MessageRequestDTO dto, long senderId, long chatId) {
        messageValidator.validateCreateMessage(dto, senderId, chatId);

        User sender = getUserOrThrow(senderId);
        Chat chat = getChatOrThrow(chatId);

        Message message = new Message();
        message.setId(messageIdGenerator.generate());
        message.setUser(sender);
        message.setChat(chat);
        message.setContent(dto.getContent());

        Message savedMessage = messageRepository.saveAndFlush(message);
        userChatUpdateService.updateUserChatOnNewMessage(chatId, savedMessage);
    }


    // 시스템 메시지 생성 및 DB 저장 [입장] //
    @Transactional
    public MessageResponseDTO createSystemEntryMessage(Chat chat, List<User> users) {
        messageValidator.validateSystemEntryMessage(chat, users);

        String userNames = users.stream()
                .filter(Objects::nonNull)
                .map(user -> (user.getName() != null && !user.getName().trim().isEmpty()) ? user.getName() : "알 수 없는 사용자")
                .collect(Collectors.joining(", "));

        if (userNames.trim().isEmpty()) {
            userNames = "새로운 사용자";
        }

        Message systemMessage = new Message();
        systemMessage.setId(messageIdGenerator.generate());
        systemMessage.setUser(null);
        systemMessage.setChat(chat);
        systemMessage.setMessageType(MessageType.SYSTEM_ENTRY);
        systemMessage.setContent(userNames + "님이 입장했습니다.");

        Message saved = messageRepository.save(systemMessage);

        return MessageResponseDTO.builder()
                .id(saved.getId())
                .senderId(0L)
                .senderName("SYSTEM")
                .chatId(chat.getId())
                .content(saved.getContent())
                .type(saved.getMessageType())
                .unReadCnt(0)
                .build();
    }


    // 시스템 메시지 생성 및 DB 저장 [퇴장] //
    @Transactional
    public MessageResponseDTO createSystemLeaveMessage(Chat chat, User leavingUser) {
        messageValidator.validateSystemLeaveMessage(chat, leavingUser);

        // 유저 이름 정보 누락 시 Fallback 처리
        String userName = (leavingUser.getName() != null && !leavingUser.getName().trim().isEmpty())
                ? leavingUser.getName()
                : "알 수 없는 사용자";

        Message systemMessage = new Message();
        systemMessage.setId(messageIdGenerator.generate());
        systemMessage.setUser(null);
        systemMessage.setChat(chat);
        systemMessage.setMessageType(MessageType.SYSTEM_LEAVE);
        systemMessage.setContent(userName + "님이 나갔습니다.");

        Message saved = messageRepository.save(systemMessage);

        return MessageResponseDTO.builder()
                .id(saved.getId())
                .senderId(0L)
                .senderName("SYSTEM")
                .chatId(chat.getId())
                .content(saved.getContent())
                .type(saved.getMessageType())
                .unReadCnt(0)
                .build();
    }


    // == 메세지 읽음 상태 업데이트 API == //
    public void updateLastReadMessage (LastReadMessageRequestDTO dto, Long curUserId, Long chatId) {
        messageValidator.validateReadRequest(dto, curUserId, chatId);

        String LAST_READ_KEY_TEMPLATE = "lastRead:user:%d:chat:%d";
        String DIRTY_SET_KEY = "lastRead:dirty_keys";

        String LAST_READ_KEY = String.format(LAST_READ_KEY_TEMPLATE, curUserId, chatId);
        Long lastMessageId = dto.getLastMessageId();

        // redis 장애 격리
        try {
            Long result = redisTemplateForString.execute(
                    lastReadUpdateScript,
                    List.of(LAST_READ_KEY, DIRTY_SET_KEY),
                    lastMessageId.toString()
            );

            // Redis 반환값 null 상태 방어
            if (result == null) {
                log.warn("[Lua-Atomic Result Null] Redis 응답이 비어있습니다. 업데이트 불명확. user={}, chat={}", curUserId, chatId);
                return;
            }
        } catch (Exception e) {
            log.error("Redis Lua script failed for user={}, chat={}, error={}", curUserId, chatId, e.getMessage());
        }
    }


    // == 메세지 목록  및 안 읽은 사람 수 반환 API == O ( M + N ) == //
    @Transactional(readOnly = true)
    public List<MessageResponseDTO> getMessageListWithUnreadCounts(Long chatId, Long userId, int page, int size) {

        messageValidator.validateMessageListParams(chatId, userId, page, size);

        UserChat userChat = userChatRepository.findReadVersionByUserIdAndChatIdAndIsDeletedFalse(userId, chatId)
                .orElseThrow(() -> {
                    log.warn("[메시지 목록 조회 실패] 참여 정보를 찾을 수 없음. userId: {}, chatId: {}", userId, chatId);
                    return new IllegalArgumentException("채팅방 참여 정보를 찾을 수 없습니다.");
                });

        LocalDateTime joinTimestamp = userChat.getCreatedAt();
        if (joinTimestamp == null) {
            log.warn("[메시지 목록 조회 주의] joinTimestamp가 null입니다. 가입일시를 현재 시간으로 대체합니다. userId: {}", userId);
            joinTimestamp = LocalDateTime.now();
        }

        // I/O 병목 해결 : Pageable 객체 생성
        Pageable pageable = PageRequest.of(
                page,
                size,
                Sort.by(Sort.Direction.ASC, "createdAt")
        );

        // M개의 메시지 조회
        Page<Message> messagePage = messageRepository.findByChatIdAndCreatedAtAfter(
                chatId,
                joinTimestamp,
                pageable
        );

        List<Message> messages = messagePage.getContent();
        if (messages.isEmpty()) {
            log.info("[메시지 목록 조회] 조회된 메시지가 없습니다. chatId: {}", chatId);
            return new ArrayList<>();
        }

        // 메세지 안읽은 사람 수 계산을 위한 참가자 정보 조회
        List<UserChat> participants = userChatRepository.findByChatIdAndIsDeletedFalseWithLastReadMessage(chatId);

        // 방 참여자가 완전히 비어있는 데이터 무결성 오류 방어
        if (participants == null || participants.isEmpty()) {
            log.error("[메시지 목록 조회 오류] 채팅방에 활성화된 참여자가 존재하지 않습니다. 무결성 깨짐. chatId: {}", chatId);
            return new ArrayList<>();
        }

        // 각 참여자의 마지막 읽은 messageId 조회 (Redis → DB fallback)
        Map<Long, Long> userLastReadMap = resolveUserLastReadMap(participants, chatId);

        // messageId별 읽은 인원 수 집계
        Map<Long, Long> readCntMap = userLastReadMap.values().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(id -> id, Collectors.counting()));

        // 메시지별 안읽은 수 누적 계산 후 DTO 조립
        return assembleMessageResponses(messages, participants.size(), readCntMap, chatId);
    }

    /*
        어떤 메시지를 몇 명이 마지막으로 읽었는지
        lastReadMessageId가 각각 (101, 103, 103, 105, 105)
        readCountMap : {101: 1L, 103: 2L, 105: 2L}
        => messageId 101 : 1명 , messageId 103 : 2명...
        ===> readCntMap를 그냥 client에 줘버리기 -> 더 효율적일수도 있음.
            클라이언트에서 핸들링하기 더 쉬움
    */
    // Redis에서 각 참여자의 lastReadMessageId 가져오기
    //      => 레디스 N + 1 발생 : 레디스에서의 시간복잡도는 바로 네트워크에 영향이 가서 매우 잘 고려해야함
    //      => MGET(pipeline 하위버전)!! 1번!!
    private Map<Long, Long> resolveUserLastReadMap(List<UserChat> participants, Long chatId) {
        List<String> redisKeys = participants.stream()
                .filter(p -> p != null && p.getUser() != null) // 내부 연관 오브젝트 NPE 사전 방어
                .map(p -> String.format("lastRead:user:%d:chat:%d", p.getUser().getId(), chatId))
                .toList();

        List<String> redisValues = null;
        try {
            // MGET으로 일괄 조회
            // MGET 이 안될 때 redispipeline 사용
            redisValues = redisTemplateForString.opsForValue().multiGet(redisKeys);
        } catch (Exception e) {
            // Redis 타임아웃/커넥션 장애 발생 시 전체 프로세스가 다운되지 않도록 격리 (DB Fallback 유도)
            log.error("❌ Redis MGET 수행 중 인프라 에러 발생. DB Fallback 데이터를 활용합니다. chatId: {}, error: {}", chatId, e.getMessage());
        }

        // Redis 응답이 null이거나 원본 참가자 리스트와 사이즈가 불일치할 경우를 대비한 무결성 동기화
        if (redisValues == null || redisValues.size() != participants.size()) {
            redisValues = new ArrayList<>(Collections.nCopies(participants.size(), (String) null));
        }

        Map<Long, Long> userLastReadMap = new HashMap<>();
        for (int i = 0; i < participants.size(); i++) {
            UserChat participant = participants.get(i);
            if (participant == null || participant.getUser() == null) continue; // NPE 패스

            String redisValue = redisValues.get(i);

            Long lastReadId = null;
            try {
                if (redisValue != null) {
                    lastReadId = Long.parseLong(redisValue);
                }
            } catch (NumberFormatException e) {
                log.error("[Redis 데이터 오염] 숫자가 아닌 대화 ID 발견. 값: {}, userId: {}", redisValue, participant.getUser().getId());
            }

            if (lastReadId == null && participant.getLastReadMessage() != null) {
                lastReadId = participant.getLastReadMessage().getId(); // DB fallback
            }

            if (lastReadId != null) {
                userLastReadMap.put(participant.getUser().getId(), lastReadId);
            }
        }

        return userLastReadMap;
    }

    private List<MessageResponseDTO> assembleMessageResponses(
            List<Message> messages, long totalParticipants, Map<Long, Long> readCntMap, Long chatId) {

        List<MessageResponseDTO> resultList = new ArrayList<>();
        long cumulativeReadCnt = 0; // 현재까지 '읽은' 사람의 누적 합계

        for (Message message : messages) {  // 시간순으로 정렬된 메시지
            if (message == null) continue; // 리스트 내부 null 요소 방어

            long messageId = message.getId();

            if (readCntMap.containsKey(messageId))
                cumulativeReadCnt += readCntMap.get(messageId);

            long unreadCnt = totalParticipants - cumulativeReadCnt;

            // 데이터 왜곡으로 인해 읽은 사람 수가 총원보다 많아져 음수가 나오는 현상 방어
            if (unreadCnt < 0) {
                unreadCnt = 0;
            }

            Long senderId;
            String senderName;

            // 메시지 타입 확인
            //      일반 대화(TALK) 메시지 => 실제 유저 정보 사용
            if (message.getMessageType() == MessageType.TALK) {
                //  TALK 타입인데 유저 정보가 누락되었거나 탈퇴한 경우
                if (message.getUser() != null) {
                    senderId = message.getUser().getId();
                    senderName = message.getUser().getName() != null ? message.getUser().getName() : "알 수 없는 사용자";
                } else {
                    log.error("[데이터 무결성 오류] TALK 타입 메시지이나 연관 User 엔티티가 null입니다. messageId: {}", messageId);
                    senderId = -1L;
                    senderName = "알 수 없는 사용자(탈퇴)";
                }
            }
            //      시스템 메시지(SYSTEM_ENTRY, SYSTEM_LEAVE 등) => 약속된 시스템 ID 사용
            else {
                senderId = 0L; // 시스템 유저 ID
                senderName = "SYSTEM";
            }

            // 연관 Chat 객체 누락 대비 Fallback 처리
            Long responseChatId = (message.getChat() != null) ? message.getChat().getId() : chatId;

            resultList.add(new MessageResponseDTO(
                    message.getId(),
                    senderId,
                    senderName,
                    responseChatId,
                    message.getContent() != null ? message.getContent() : "",
                    message.getMessageType() != null ? message.getMessageType() : MessageType.TALK,
                    (int) unreadCnt
            ));
        }

        return resultList;
    }

    // ============== //

    private User getUserOrThrow(Long userId) {
        return userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("유저 정보를 찾을 수 없습니다. id=" + userId));
    }

    private Chat getChatOrThrow(Long chatId) {
        return chatRepository.findById(chatId)
                .orElseThrow(() -> new IllegalArgumentException("채팅방 참여 정보를 찾을 수 없습니다. id=" + chatId));
    }
}