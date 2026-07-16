package com.dy.minichat.global.infra.datasource;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterReplicaKeyRouter {
    @Getter
    private String masterKey = "";
    private final List<String> replicaKeys = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger(0);

    public void setMasterKey(String masterKey) {
        if (masterKey == null || masterKey.isBlank()) {
            throw new IllegalArgumentException("masterKey는 비어있을 수 없습니다.");
        }
        this.masterKey = masterKey;
    }

    public void addReplicaKey(String key) {
        replicaKeys.add(key);
    }

    public String getReplicaKey() {

        // 레플리카도 없고 마스터키도 잘못돼서 요청이 죽는 상황 방지
        if (replicaKeys.isEmpty()) {
            return masterKey;
        }
        int current = counter.updateAndGet(i -> i >= replicaKeys.size() - 1 ? 0 : i + 1);
        return replicaKeys.get(current);
    }
}