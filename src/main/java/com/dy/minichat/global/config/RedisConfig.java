package com.dy.minichat.global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

@Configuration
public class RedisConfig {
    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        LettuceClientConfiguration cfg = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofSeconds(2))
                .build();
        return new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(host, port), cfg);
    }


    @Bean
    public RedisTemplate<String, Long> redisTemplateForLong () {
        RedisTemplate<String, Long> template = new RedisTemplate<>();

        template.setConnectionFactory(redisConnectionFactory());
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RedisTemplate<String, String> redisTemplateForString () {
        RedisTemplate<String, String> template = new RedisTemplate<>();

        template.setConnectionFactory(redisConnectionFactory());

        // Key, Value 직렬화(Serialization) 방식을 String으로 설정
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());

        // Hash Key, Hash Value의 직렬화 방식도 String으로 설정
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());

        // 모든 설정이 완료되면 template을 초기화
        template.afterPropertiesSet();

        return template;
    }

    @Bean
    public RedisScript<Long> lastReadUpdateScript() {
        String script =
                // KEYS[1]:
                // KEYS[2]: Dirty Set
                // ARGV[1]:
                "local current = redis.call('GET', KEYS[1]) " +
                        "if not current or tonumber(ARGV[1]) > tonumber(current) then " + //
                        "redis.call('SET', KEYS[1], ARGV[1]) " +
                        "redis.call('SADD', KEYS[2], KEYS[1]) " +
                        "return 1 " + // 1 (
                        "end " +
                        "return 0"; // 0 (

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script);
        redisScript.setResultType(Long.class);
        return redisScript;
    }

    // RedisConfig.java 내부에 추가

    @Bean
    public RedisScript<Long> rateLimitScript() {
        String script =
                // KEYS[1]: 유저별 Sorted Set 키
                // ARGV[1]: 현재 시각 (ms)
                // ARGV[2]: 윈도우 크기 (ms)
                // ARGV[3]: 허용 요청 수
                // ARGV[4]: 현재 요청 고유 ID

                "local key = KEYS[1] " +
                        "local current_time = tonumber(ARGV[1]) " +
                        "local window_size = tonumber(ARGV[2]) " +
                        "local limit = tonumber(ARGV[3]) " +
                        "local member = ARGV[4] " +

                        "local window_start = current_time - window_size " +

                        // 1. 윈도우 이전의 오래된 로그 삭제
                        "redis.call('ZREMRANGEBYSCORE', key, 0, window_start) " +

                        // 2. 현재 윈도우 내의 요청 수 확인
                        "local count = redis.call('ZCARD', key) " +

                        // 3. 한도 체크
                        "if count >= limit then " +
                        "  return 1 " + // 1 (한도 초과)
                        "end " +

                        // 4. 현재 요청을 로그로 추가
                        "redis.call('ZADD', key, current_time, member) " +
                        "redis.call('EXPIRE', key, math.ceil(window_size / 1000) + 5) " +

                        "return 0"; // 0 (한도 이내)

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script); // .setLocation() 대신 .setScriptText() 사용
        redisScript.setResultType(Long.class);
        return redisScript;
    }
}