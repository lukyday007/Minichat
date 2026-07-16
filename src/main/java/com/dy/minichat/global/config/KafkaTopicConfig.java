package com.dy.minichat.global.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    /**
     * 'user-chat-update' 토픽을 파티션 3개로 생성
     */
    @Bean
    public NewTopic userChatUpdateTopic() {
        return TopicBuilder.name("user-chat-update")
                .partitions(3) // <-- 여기에 3개 설정
                .replicas(1)   // (로컬 테스트는 1, 운영은 2 or 3 권장)
                .build();
    }

    /**
     * 'chat-message' 토픽을 파티션 3개로 생성
     */
    @Bean
    public NewTopic chatMessageTopic() {
        return TopicBuilder.name("chat-message")
                .partitions(3) // <-- 여기에 3개 설정
                .replicas(1)
                .build();
    }
}