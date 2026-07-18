package com.dy.minichat.service;

import com.dy.minichat.global.security.JwtTokenProvider;
import com.dy.minichat.global.id.UserIdGenerator;
import com.dy.minichat.dto.request.LoginRequestDTO;
import com.dy.minichat.dto.request.SignUpRequestDTO;
import com.dy.minichat.dto.response.LoginResponseDTO;
import com.dy.minichat.entity.User;
import com.dy.minichat.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final UserIdGenerator userIdGenerator;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;


    @Transactional
    public User signUp(SignUpRequestDTO dto) {

        if (dto == null || dto.getEmail() == null || dto.getPassword() == null) {
            throw new IllegalArgumentException("필수 가입 정보가 누락되었습니다.");
        }
        if (userRepository.existsByEmail(dto.getEmail())) {
            throw new IllegalArgumentException("이미 사용 중인 ID입니다.");
        }

        String encodedPassword = passwordEncoder.encode(dto.getPassword());

        User user = new User();
        user.setId(userIdGenerator.generate());
        // user: snowflake id generate
        user.setEmail(dto.getEmail());
        user.setPassword(encodedPassword);
        user.setName(dto.getName());

        return userRepository.save(user);
    }


    @Transactional
    public LoginResponseDTO login(LoginRequestDTO requestDTO) {

        if (requestDTO == null || requestDTO.getEmail() == null || requestDTO.getPassword() == null) {
            throw new IllegalArgumentException("이메일과 비밀번호를 모두 입력해주세요.");
        }

        User user = (User) userRepository.findByEmail(requestDTO.getEmail())
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 이메일입니다."));

        if (!passwordEncoder.matches(requestDTO.getPassword(), user.getPassword())) {
            throw new IllegalArgumentException("비밀번호가 일치하지 않습니다.");
        }

        // Access & Refresh Token 생성
        String accessToken = jwtTokenProvider.generateAccessToken(user.getId());
        String refreshToken = jwtTokenProvider.generateRefreshToken(user.getId());

        // Refresh Token 저장 안함 — Stateless 유지
        return LoginResponseDTO.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .userId(user.getId())
                .build();

    }


    @Transactional(readOnly = true)
    public LoginResponseDTO reissue(String refreshToken) {

        if (refreshToken == null || refreshToken.trim().isEmpty()) {
            throw new IllegalArgumentException("Refresh Token이 인증 정보에 포함되어 있지 않습니다.");
        }

        // Refresh Token 검증
        if (!jwtTokenProvider.validateToken(refreshToken)) {
            throw new IllegalArgumentException("유효하지 않은 Refresh Token");
        }

        // Refresh Token에서 userId 추출
        Long userId = jwtTokenProvider.getUserIdFromToken(refreshToken);

        // 3사용자 존재 여부 확인
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("사용자를 찾을 수 없습니다."));

        // 새 Access Token 발급
        String newAccessToken = jwtTokenProvider.generateAccessToken(userId);

        // 새 Refresh Token 발급 (선택 — rotation 필요하면)
        String newRefreshToken = jwtTokenProvider.generateRefreshToken(userId);

        return LoginResponseDTO.builder()
                .accessToken(newAccessToken)
                .refreshToken(newRefreshToken)
                .userId(userId)
                .build();
    }
}