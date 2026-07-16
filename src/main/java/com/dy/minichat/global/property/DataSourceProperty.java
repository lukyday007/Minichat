package com.dy.minichat.global.property;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Slf4j
@ToString
@Setter
@Getter
@ConfigurationProperties(prefix = "spring.datasource")
public class DataSourceProperty {

    private String username;
    private String password;
    private DatabaseInfo master;
    private List<DatabaseInfo> replicas;
    @ToString
    @Setter
    @Getter
    @RequiredArgsConstructor
    public static class DatabaseInfo {
        private String key;
        private String url;
    }

    @PostConstruct
    void logOnStart() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Loaded DataSourceProperty ===\n")
                .append("  username=").append(username)
                .append(", password=").append(password).append("\n")
                .append("  master=").append(master.getKey())
                .append(" (").append(master.getUrl()).append(")\n")
                .append("  slaves=[");

        if (replicas != null) {
            sb.append(replicas.stream()
                    .map(DatabaseInfo::getKey)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
        }
        sb.append("]");
        log.info(sb.toString());
    }
}