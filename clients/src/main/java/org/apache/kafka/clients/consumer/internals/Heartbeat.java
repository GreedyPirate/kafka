/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

/**
 * 这只是一个VO类
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    // 参数
    private final int sessionTimeoutMs;
    private final int heartbeatIntervalMs;
    private final int maxPollIntervalMs;
    private final long retryBackoffMs;

    // 上一次心跳发送时间，volatile用于监控读取
    private volatile long lastHeartbeatSend; // volatile since it is read by metrics
    // 上一次心跳响应接收时间
    private long lastHeartbeatReceive;
    // session重置/初始化的实际
    private long lastSessionReset;
    // consumer 上一次poll的实际
    private long lastPoll;
    private boolean heartbeatFailed;

    public Heartbeat(int sessionTimeoutMs,
                     int heartbeatIntervalMs,
                     int maxPollIntervalMs,
                     long retryBackoffMs) {
        if (heartbeatIntervalMs >= sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxPollIntervalMs = maxPollIntervalMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    public void poll(long now) {
        this.lastPoll = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
        this.heartbeatFailed = false;
    }

    public void failHeartbeat() {
        this.heartbeatFailed = true;
    }

    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    /**
     * 计算距离下一次心跳的剩余时间，比如还有3秒就要做下一次心跳
     */
    public long timeToNextHeartbeat(long now) {
        // 距离上一次心跳的时间
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);

        // 计划下一次心跳的时间
        final long delayToNextHeartbeat;
        if (heartbeatFailed)
            // 失败，就是重试间隔
            delayToNextHeartbeat = retryBackoffMs;
        else
            // 正常的interval时间
            delayToNextHeartbeat = heartbeatIntervalMs;

        // 已经超出了计划心跳时间，需要立即心跳
        if (timeSinceLastHeartbeat > delayToNextHeartbeat)
            return 0;
        else
            // 按计划还有几秒进行下一次心跳
            return delayToNextHeartbeat - timeSinceLastHeartbeat;
    }

    public boolean sessionTimeoutExpired(long now) {
        // 距离上次发送心跳成功的时间 是否大于sessionTimeout
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > sessionTimeoutMs;
    }

    public long interval() {
        return heartbeatIntervalMs;
    }

    public void resetTimeouts(long now) {
        this.lastSessionReset = now;
        this.lastPoll = now;
        this.heartbeatFailed = false;
    }

    public boolean pollTimeoutExpired(long now) {
        // maxPollInterval是否过期
        return now - lastPoll > maxPollIntervalMs;
    }

    public long lastPollTime() {
        return lastPoll;
    }

}