package com.dy.minichat.global.security;

import io.jsonwebtoken.ExpiredJwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private final JwtTokenProvider jwtTokenProvider;
    private final JwtAuthenticationEntryPoint jwtAuthenticationEntryPoint;

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        String accessToken = extractAccessToken(request);

        if (StringUtils.hasText(accessToken)) {
            try {
                Long userId = jwtTokenProvider.getUserIdFromToken(accessToken);
                setAuthentication(userId);

            } catch (ExpiredJwtException ex) {
                // Access token expired -> Respond 401 (client should call /refresh)
                log.debug("[AuthFilter] Access token expired for request: {}", request.getRequestURI());
                jwtAuthenticationEntryPoint.commence(request, response, null);
                return;

            } catch (Exception ex) {
                // invalid signature, malformed, etc -> 401
                log.warn("[AuthFilter] Invalid access token: {}", ex.getMessage());
                jwtAuthenticationEntryPoint.commence(request, response, null);
                return;
            }
        }

        filterChain.doFilter(request, response);
    }

    private void setAuthentication(Long userId) {
        UsernamePasswordAuthenticationToken authentication =
                new UsernamePasswordAuthenticationToken(userId, null, Collections.emptyList());
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    private String extractAccessToken(HttpServletRequest request) {
        String bearer = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (StringUtils.hasText(bearer) && bearer.startsWith("Bearer ")) {
            return bearer.substring(7);
        }
        return null;
    }
}