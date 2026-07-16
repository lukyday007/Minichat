package com.dy.minichat.component;

import io.jsonwebtoken.*;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.util.Date;

@Slf4j
@Component
public class JwtTokenProvider {

    private final Key key;
    private final long ACCESS_TOKEN_EXP = 1000L * 60 * 60;        // 1시간
    private final long REFRESH_TOKEN_EXP = 1000L * 60 * 60 * 24 * 7; // 7일

    // session vs JWT(MSA)

    // application.yml에서 secret 값 가져오기
    public JwtTokenProvider(@Value("${jwt.secret}") String secretKey) {
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        this.key = Keys.hmacShaKeyFor(keyBytes);
    }
    /**
     * 사용자 ID를 기반으로 Access Token 생성
     */
    public String generateAccessToken(Long userId) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + ACCESS_TOKEN_EXP);

        return Jwts.builder()
                .setSubject(String.valueOf(userId)) // (사용자 ID)
                .setIssuedAt(now)                   // 발행 시간
                .setExpiration(expiryDate)          // 만료 시간
                .signWith(key)                      // [수정] 이미 HS256이므로 key만 사용
                .compact();
    }

    /**
     * 사용자 ID를 기반 Refresh Token 생성
     */
    public String generateRefreshToken(Long userId) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + REFRESH_TOKEN_EXP);

        return Jwts.builder()
                .setSubject(String.valueOf(userId))
                .setIssuedAt(now)
                .setExpiration(expiryDate)
                .signWith(key)
                .compact();
    }


    /**
     * 토큰 유효성 검증
     */
    public boolean validateToken(String token) {
        try {
            Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(token);
            return true;
        } catch (SignatureException e) {
            log.error("Invalid JWT signature: {}", e.getMessage());
        } catch (MalformedJwtException e) {
            log.error("Invalid JWT token: {}", e.getMessage());
        } catch (ExpiredJwtException e) {
            // [참고] 만료된 토큰도 유효하지 않은 것으로 간주 (정상)
            log.error("JWT token is expired: {}", e.getMessage());
        } catch (UnsupportedJwtException e) {
            log.error("JWT token is unsupported: {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            log.error("JWT claims string is empty: {}", e.getMessage());
        }
        return false;
    }

    /**
     * 토큰에서 사용자 ID (Subject)를 추출.
     * (만료된 토큰의 경우 ExpiredJwtException이 발생)
     */
    public Long getUserIdFromToken(String token) {
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token) // [참고] 만료 시 여기서 예외 발생
                .getBody();

        return Long.parseLong(claims.getSubject());
    }
}