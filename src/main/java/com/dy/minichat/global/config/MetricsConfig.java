package com.dy.minichat.global.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    /**
     * 히스토그램 버킷 전면 비활성화
     * 고부하 시 버킷 스냅샷 경합으로 /actuator/prometheus 가 500을 반환하는 문제 차단
     * count / sum / max 는 그대로 유지
     */
    @Bean
    public MeterFilter disableHistogramBuckets() {
        return new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                return DistributionStatisticConfig.builder()
                        .percentilesHistogram(false)
                        .serviceLevelObjectives()
                        .build()
                        .merge(config);
            }
        };
    }
}