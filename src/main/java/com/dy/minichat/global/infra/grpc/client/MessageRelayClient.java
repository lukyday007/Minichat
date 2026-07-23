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

        RelayMessageServiceGrpc.RelayMessageServiceBlockingStub stub = RelayMessageServiceGrpc.newBlockingStub(channel);

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

        RelayMessageServiceGrpc.RelayMessageServiceBlockingStub stub = RelayMessageServiceGrpc.newBlockingStub(channel);

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
            // log.info("gRPC Bulk Relay Result: {}", response.getMessage());

        } catch (Exception e) {
            log.error("gRPC 벌크 릴레이 실패. 대상: {}, 원인: {}", targetAddress, e.getMessage());
        }
    }

    @PreDestroy
    public void shutdownAllChannels() {
        // log.info("애플리케이션 종료: 모든 활성 gRPC 채널을 종료합니다...");
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown();
            } catch (Exception e) {
                log.error("gRPC 채널 종료 중 에러 발생", e);
            }
        }
        // log.info("모든 gRPC 채널이 종료되었습니다.");
    }
}