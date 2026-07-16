package com.dy.minichat.global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServerConfig {
    @Value("${server.identifier}")
    private String identifier;

    @Bean
    public String serverIdentifier() {
        // 주입받은 값을 그대로 Bean으로 등록
        return identifier;
    }
}