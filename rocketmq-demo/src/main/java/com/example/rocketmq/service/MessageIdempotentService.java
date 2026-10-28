package com.example.rocketmq.service;

import com.example.rocketmq.constant.RedisKeyConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 消息幂等性服务
 *
 * 功能：防止消息重复消费
 *
 * 实现原理：
 * 1. 使用Redis存储已处理的消息ID
 * 2. 消费前检查是否已处理
 * 3. 消费成功后标记已处理
 *
 * @author example
 * 
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageIdempotentService {

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 检查消息是否已处理
     *
     * @param messageId 消息ID（全局唯一）
     * @param businessKey 业务Key（如订单号）
     * @return true-已处理，false-未处理
     */
    public boolean isProcessed(String messageId, String businessKey) {
        String key = RedisKeyConstant.buildIdempotentKey(messageId, businessKey);
        Boolean hasKey = stringRedisTemplate.hasKey(key);
        return Boolean.TRUE.equals(hasKey);
    }

    /**
     * 标记消息已处理
     *
     * @param messageId 消息ID
     * @param businessKey 业务Key
     */
    public void markProcessed(String messageId, String businessKey) {
        String key = RedisKeyConstant.buildIdempotentKey(messageId, businessKey);
        stringRedisTemplate.opsForValue().set(
                key,
                "1",
                RedisKeyConstant.IDEMPOTENT_EXPIRE_SECONDS,
                TimeUnit.SECONDS
        );
        log.debug("标记消息已处理 - MessageId: {}, BusinessKey: {}", messageId, businessKey);
    }

    /**
     * 记录失败次数
     *
     * @param businessKey 业务Key
     */
    public void recordFailure(String businessKey) {
        String key = RedisKeyConstant.buildFailCountKey(businessKey);
        stringRedisTemplate.opsForValue().increment(key);
        stringRedisTemplate.expire(key, RedisKeyConstant.FAIL_COUNT_EXPIRE_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * 获取失败次数
     *
     * @param businessKey 业务Key
     * @return 失败次数
     */
    public int getFailureCount(String businessKey) {
        String key = RedisKeyConstant.buildFailCountKey(businessKey);
        String value = stringRedisTemplate.opsForValue().get(key);
        return value != null ? Integer.parseInt(value) : 0;
    }
}