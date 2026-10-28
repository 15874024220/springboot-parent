package com.example.rocketmq.constant;

import java.util.concurrent.TimeUnit;

/**
 * Redis Key常量定义类
 *
 * 统一管理Redis Key的格式和过期时间
 *
 * @author example
 * 
 */
public class RedisKeyConstant {

    // ==================== Key前缀 ====================
    /** 消息幂等性Key前缀 */
    public static final String MSG_IDEMPOTENT_PREFIX = "mq:idempotent:";

    /** 订单分布式锁Key前缀 */
    public static final String ORDER_LOCK_PREFIX = "order:lock:";

    /** 消息失败计数Key前缀 */
    public static final String MSG_FAIL_COUNT_PREFIX = "mq:fail:count:";

    /** 订单缓存Key前缀 */
    public static final String ORDER_CACHE_PREFIX = "order:cache:";

    // ==================== 过期时间（秒） ====================
    /** 幂等性Key过期时间（7天） */
    public static final long IDEMPOTENT_EXPIRE_SECONDS = TimeUnit.DAYS.toSeconds(7);

    /** 分布式锁过期时间（30秒） */
    public static final long LOCK_EXPIRE_SECONDS = 30;

    /** 失败计数过期时间（1天） */
    public static final long FAIL_COUNT_EXPIRE_SECONDS = TimeUnit.DAYS.toSeconds(1);

    /** 订单缓存过期时间（30分钟） */
    public static final long ORDER_CACHE_EXPIRE_SECONDS = TimeUnit.MINUTES.toSeconds(30);

    /**
     * 构建幂等性Key
     * @param messageId 消息ID
     * @param businessKey 业务Key
     * @return Redis Key
     */
    public static String buildIdempotentKey(String messageId, String businessKey) {
        return MSG_IDEMPOTENT_PREFIX + businessKey + ":" + messageId;
    }

    /**
     * 构建订单锁Key
     * @param orderId 订单ID
     * @return Redis Key
     */
    public static String buildOrderLockKey(String orderId) {
        return ORDER_LOCK_PREFIX + orderId;
    }

    /**
     * 构建失败计数Key
     * @param orderId 订单ID
     * @return Redis Key
     */
    public static String buildFailCountKey(String orderId) {
        return MSG_FAIL_COUNT_PREFIX + orderId;
    }

    /**
     * 构建订单缓存Key
     * @param orderId 订单ID
     * @return Redis Key
     */
    public static String buildOrderCacheKey(String orderId) {
        return ORDER_CACHE_PREFIX + orderId;
    }
}