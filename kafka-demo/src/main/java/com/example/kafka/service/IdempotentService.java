package com.example.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 幂等服务
 *
 * 核心功能：
 * 1. 防止消息重复消费
 * 2. 使用 Redis 存储已处理的消息ID
 * 3. 支持设置过期时间
 *
 * 实现原理：
 * - 消费者收到消息时，先检查消息ID是否已存在
 * - 不存在则处理业务，并标记为已处理
 * - 存在则跳过处理
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotentService {

    private final StringRedisTemplate redisTemplate;

    // 幂等键前缀
    private static final String IDEMPOTENT_KEY_PREFIX = "kafka:processed:";

    // 幂等记录过期时间（默认7天）
    private static final long EXPIRE_HOURS = 24 * 7;

    /**
     * 检查消息是否已被处理
     *
     * @param messageId 消息唯一标识
     * @return true=已处理过，需要跳过
     */
    public boolean isProcessed(String messageId) {
        String key = buildKey(messageId);
        Boolean exists = redisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * 标记消息为已处理
     *
     * @param messageId 消息唯一标识
     */
    public void markProcessed(String messageId) {
        String key = buildKey(messageId);
        // 使用 SETNX + 过期时间
        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(key, String.valueOf(System.currentTimeMillis()),
                        EXPIRE_HOURS, TimeUnit.HOURS);

        if (Boolean.TRUE.equals(success)) {
            log.debug("消息已标记为已处理: messageId={}", messageId);
        } else {
            // 可能并发场景下重复标记
            log.debug("消息已被其他线程标记: messageId={}", messageId);
        }
    }

    /**
     * 删除幂等记录（用于补偿场景）
     */
    public void removeProcessed(String messageId) {
        String key = buildKey(messageId);
        redisTemplate.delete(key);
        log.debug("删除幂等记录: messageId={}", messageId);
    }

    /**
     * 使用业务ID作为幂等键（适用于同一业务只处理一次的场景）
     */
    public boolean isBusinessProcessed(String businessId) {
        String key = IDEMPOTENT_KEY_PREFIX + "business:" + businessId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    public void markBusinessProcessed(String businessId) {
        String key = IDEMPOTENT_KEY_PREFIX + "business:" + businessId;
        redisTemplate.opsForValue().set(key, "1", EXPIRE_HOURS, TimeUnit.HOURS);
    }

    private String buildKey(String messageId) {
        return IDEMPOTENT_KEY_PREFIX + messageId;
    }
}