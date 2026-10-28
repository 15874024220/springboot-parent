package com.example.kafka.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

/**
 * Kafka 工具类
 */
@Slf4j
public class KafkaUtils {

    /**
     * 生成消息ID
     */
    public static String generateMessageId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成业务ID
     */
    public static String generateBusinessId(String prefix) {
        return prefix + "-" + System.currentTimeMillis();
    }

    /**
     * 获取消息的TraceId（用于链路追踪）
     */
    public static String getTraceId(ConsumerRecord<?, ?> record) {
        if (record.headers() != null) {
            org.apache.kafka.common.header.Header header =
                    record.headers().lastHeader("traceId");
            if (header != null) {
                return new String(header.value());
            }
        }
        return generateMessageId();
    }

    /**
     * 添加追踪头信息
     */
    public static ProducerRecord<String, Object> addTraceHeaders(
            ProducerRecord<String, Object> record, String traceId) {
        record.headers().add("traceId", traceId.getBytes());
        record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        return record;
    }

    /**
     * 获取重试次数
     */
    public static int getRetryCount(ConsumerRecord<?, ?> record) {
        if (record.headers() != null) {
            org.apache.kafka.common.header.Header header =
                    record.headers().lastHeader("retryCount");
            if (header != null) {
                try {
                    return Integer.parseInt(new String(header.value()));
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }
}