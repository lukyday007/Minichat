package com.dy.minichat.global.property;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties(prefix = "grpc.server")
@Getter
@Setter
public class GrpcServerProperties {
    // Key: "local1", Value: "localhost:9091"
    private Map<String, String> addresses;
}