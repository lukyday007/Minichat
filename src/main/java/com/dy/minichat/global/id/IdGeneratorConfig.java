package com.dy.minichat.global.id;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IdGeneratorConfig {

    @Value("${server.node-id}")
    private long nodeId;

    @Bean
    public Snowflake snowflake() {
        return new Snowflake(nodeId);
    }

    @Bean
    public UserIdGenerator userIdGenerator(Snowflake snowflake) {
        return new UserIdGenerator(snowflake);
    }

    @Bean
    public ChatIdGenerator chatIdGenerator(Snowflake snowflake) {
        return new ChatIdGenerator(snowflake);
    }

    @Bean
    public MessageIdGenerator messageIdGenerator(Snowflake snowflake) {
        return new MessageIdGenerator(snowflake);
    }

    @Bean
    public UserChatIdGenerator userChatIdGenerator(Snowflake snowflake) {
        return new UserChatIdGenerator(snowflake);
    }
}