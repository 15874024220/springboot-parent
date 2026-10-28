package com.example.kafka.constant;

/**
 * Kafka 常量定义
 */
public class KafkaConstants {

    // ==================== 主题名称 ====================
    public static final String TOPIC_NORMAL = "normal-topic";
    public static final String TOPIC_ORDERED = "ordered-topic";
    public static final String TOPIC_TRANSACTIONAL = "transactional-topic";
    public static final String TOPIC_DEAD_LETTER = "dead-letter-topic";

    // ==================== 消费者组 ====================
    public static final String GROUP_ID_NORMAL = "normal-consumer-group";
    public static final String GROUP_ID_ORDERED = "ordered-consumer-group";
    public static final String GROUP_ID_TRANSACTIONAL = "transactional-consumer-group";

    // ==================== 消息头 ====================
    public static final String HEADER_MESSAGE_ID = "messageId";
    public static final String HEADER_BUSINESS_ID = "businessId";
    public static final String HEADER_TRACE_ID = "traceId";
    public static final String HEADER_RETRY_COUNT = "retryCount";
    public static final String HEADER_SOURCE = "source";
    public static final String HEADER_TIMESTAMP = "timestamp";

    // ==================== Redis Key前缀 ====================
    public static final String REDIS_KEY_IDEMPOTENT = "kafka:idempotent:";
    public static final String REDIS_KEY_RETRY = "kafka:retry:";
    public static final String REDIS_KEY_DEAD_LETTER = "kafka:dead-letter:";

    // ==================== 错误码 ====================
    public static final int ERROR_CODE_RETRYABLE = 1001;     // 可重试错误
    public static final int ERROR_CODE_NON_RETRYABLE = 1002; // 不可重试错误
    public static final int ERROR_CODE_TIMEOUT = 1003;       // 超时错误
    public static final int ERROR_CODE_BUSINESS = 1004;      // 业务错误

    // ==================== 默认配置 ====================
    public static final int DEFAULT_RETRY_ATTEMPTS = 3;
    public static final int DEFAULT_RETRY_BACKOFF_MS = 1000;
    public static final int DEFAULT_POLL_TIMEOUT_MS = 5000;
}