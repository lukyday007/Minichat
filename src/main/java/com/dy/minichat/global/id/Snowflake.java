package com.dy.minichat.global.id;

public class Snowflake {
    private static final int UNUSED_SIGN_BITS = 1;
    private static final int EPOCH_BITS = 41;
    private static final int NODE_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;
    private static final long DEFAULT_CUSTOM_EPOCH = 1735689600000L; // 2025-01-01 00:00:00 UTC

    // 최대 허용 범위 마스크 (자바 기본 비트 연산)
    private static final long MAX_NODE_ID = (1L << NODE_ID_BITS) - 1; // 1023
    private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1; // 4095

    private final long nodeId;
    private final long customEpoch;

    private volatile long lastTimestamp = -1L;
    private volatile long sequence = 0L;

    public Snowflake(long nodeId) {

        // NodeID 검증 가드 코드 추가: 분산 환경에서 1023(10bit)을 넘어가면 예외 발생
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException("nodeId는 0에서 " + MAX_NODE_ID + " 사이여야 합니다.");
        }

        this.nodeId = nodeId;
        this.customEpoch = DEFAULT_CUSTOM_EPOCH;
    }

    public synchronized long nextId() {
        long currentTimestamp = System.currentTimeMillis() - customEpoch;

        // Clock Drift 대응: 시계가 뒤로 가면 에러를 던져 ID 중복/역행 원천 차단
        if (currentTimestamp < lastTimestamp) {
            throw new RuntimeException(String.format(
                    "Clock moved backwards. Refusing to generate id for %d milliseconds",
                    lastTimestamp - currentTimestamp
            ));
        }

        // 동일 밀리초 내 오버플로우 대응
        if (currentTimestamp == lastTimestamp) {
            // 비트 마스킹(&) 연산으로 안전하게 시퀀스 증가 (4095 넘어가면 0이 됨)
            sequence = (sequence + 1) & SEQUENCE_MASK;

            // 시퀀스가 0이 되었다는 것은 4096개 제한을 초과했다는 의미 -> 다음 밀리초까지 대기
            if (sequence == 0) {
                currentTimestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 시간이 정상적으로 흐르면 시퀀스 초기화
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;

        return currentTimestamp << (NODE_ID_BITS + SEQUENCE_BITS)
                | (nodeId << SEQUENCE_BITS)
                | sequence;
    }

    // 다음 밀리초가 될 때까지 루프 돌며 대기하는 표준 메서드
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis() - customEpoch;
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis() - customEpoch;
        }
        return timestamp;
    }

    public long[] parse(long id) {
        long maskNodeId = ((1L << NODE_ID_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;

        long timestamp = (id >> (NODE_ID_BITS + SEQUENCE_BITS)) + customEpoch;
        long nodeId = (id & maskNodeId) >> SEQUENCE_BITS;
        long sequence = id & maskSequence;

        return new long[]{timestamp, nodeId, sequence};
    }
}