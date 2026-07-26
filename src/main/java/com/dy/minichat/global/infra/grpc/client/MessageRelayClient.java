package com.dy.minichat.global.infra.grpc.client;

import com.dy.grpc.proto.RelayBulkMessageRequest;
import com.dy.grpc.proto.RelayMessageResponse;
import com.dy.grpc.proto.RelayMessageServiceGrpc;
import com.dy.grpc.proto.RelayMessageRequest;
import com.dy.minichat.dto.message.TalkMessageDTO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/*
    서버 (송신 측) grpc client 코드
*/
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageRelayClient {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();


    public void relayMessageToServer (String targetServerHost, int targetServerPort, TalkMessageDTO messageDto, Long recipientId) {
        String targetAddress = targetServerHost + ":" + targetServerPort;
        ManagedChannel channel = channels.computeIfAbsent(targetAddress, key ->
                ManagedChannelBuilder.forAddress(targetServerHost, targetServerPort)
                        .usePlaintext()
                        .build()
        );

        // deadline이 없을 경우 -> blocking stub이 스레드를 무한정 붙잡음
        //                   => 스레드풀 고갈 → 전체 장애 전파
        RelayMessageServiceGrpc.RelayMessageServiceBlockingStub stub =
                RelayMessageServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(2, TimeUnit.SECONDS);

        RelayMessageRequest request = RelayMessageRequest.newBuilder()
                .setSenderId(messageDto.getSenderId())
                .setChatId(messageDto.getChatId())
                .setContent(messageDto.getContent())
                .setMessageType(messageDto.getType().name())
                .setTimestamp(messageDto.getTimestamp().toString())
                .setRecipientId(recipientId)
                .build();

        try {
            RelayMessageResponse response = stub.relayMessage(request);
        } catch (Exception e) {
            log.error("gRPC 단건 릴레이 실패. 대상: {}, 원인: {}", targetAddress, e.getMessage());
        }
    }


    /**
     * [추가] 벌크 릴레이 메서드
     */
    public void relayBulkMessageToServer(String targetServerHost, int targetServerPort, TalkMessageDTO messageDto, List<Long> recipientIds) {
        String targetAddress = targetServerHost + ":" + targetServerPort;
        ManagedChannel channel = channels.computeIfAbsent(targetAddress, key ->
                ManagedChannelBuilder.forAddress(targetServerHost, targetServerPort)
                        .usePlaintext()
                        .build()
        );

        // deadline이 없을 경우  -> blocking stub이 스레드를 무한정 붙잡음
        //                    => 스레드풀 고갈 → 전체 장애 전파
        RelayMessageServiceGrpc.RelayMessageServiceBlockingStub stub =
                RelayMessageServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(2, TimeUnit.SECONDS);

        RelayBulkMessageRequest request = RelayBulkMessageRequest.newBuilder()
                .setSenderId(messageDto.getSenderId())
                .setChatId(messageDto.getChatId())
                .setContent(messageDto.getContent())
                .setMessageType(messageDto.getType().name())
                .setTimestamp(messageDto.getTimestamp().toString())
                .addAllRecipientIds(recipientIds)
                .build();

        try {
            RelayMessageResponse response = stub.relayBulkMessage(request);
        } catch (Exception e) {
            log.error("gRPC 벌크 릴레이 실패. 대상: {}, 원인: {}", targetAddress, e.getMessage());
        }
    }


    @PreDestroy
    public void shutdownAllChannels() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            String targetAddress = entry.getKey();
            ManagedChannel channel = entry.getValue();

            try {
                // 1. 새로운 RPC 호출 거부 (기존 진행 중인 RPC는 계속 수행)
                channel.shutdown();

                // 2. 진행 중인 RPC가 완료될 때까지 최대 5초간 대기
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("gRPC 채널이 제한 시간(5초) 내에 정상 종료되지 않아 강제 종료합니다. 대상: {}", targetAddress);
                    // 3. 타임아웃 초과 시 강제 종료
                    channel.shutdownNow();

                    // 강제 종료 후 추가 대기 (선택 사항)
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("gRPC 채널 강제 종료 실패. 대상: {}", targetAddress);
                    }
                }
            } catch (InterruptedException e) {
                log.warn("gRPC 채널 종료 대기 중 인터럽트가 발생하여 강제 종료합니다. 대상: {}", targetAddress);
                channel.shutdownNow();
                Thread.currentThread().interrupt(); // 스레드 인터럽트 상태 복원
            } catch (Exception e) {
                log.error("gRPC 채널 종료 중 예외 발생. 대상: {}", targetAddress, e);
            }
        }
    }
}