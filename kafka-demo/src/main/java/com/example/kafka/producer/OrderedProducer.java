package com.example.kafka.producer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 顺序消息生产者
 * 核心原理：相同业务ID的消息发送到同一个分区
 * Kafka保证同一分区内消息的有序性
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderedProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.ordered}")
    private String orderedTopic;

    /**
     * 发送顺序消息
     *
     * 关键点：
     * 1. 使用业务ID作为消息的KEY
     * 2. 相同的KEY会被Kafka的分区器路由到同一个分区
     * 3. 使用同步发送确保消息按发送顺序到达
     *
     * @param businessId 业务ID（如订单ID、用户ID）
     * @param payload 消息内容
     */
    public void sendOrderedMessage(String businessId, Object payload) {
        // 重要：使用业务ID作为key，确保同一业务的消息进入同一分区
        String key = businessId;

        Message<Object> message = Message.builder()
                .key(key)
                .businessId(businessId)
                .payload(payload)
                .source("ordered-producer")
                .build();

        // 方案一：同步发送（保证顺序，但性能较低）
        try {
            // 同步发送确保消息按顺序到达Broker
            kafkaTemplate.send(orderedTopic, key, message).get();
            log.info("顺序消息发送成功: businessId={}, key={}", businessId, key);
        } catch (Exception e) {
            log.error("顺序消息发送失败: businessId={}", businessId, e);
            throw new RuntimeException("顺序消息发送失败", e);
        }

        /*
         * 方案二：异步发送 + 分区器配置（性能更好）
         * 需要配合自定义分区器使用
         */
        // kafkaTemplate.send(orderedTopic, key, message)
        //     .addCallback(result -> {
        //         log.info("顺序消息异步发送成功");
        //     }, ex -> {
        //         log.error("顺序消息异步发送失败", ex);
        //     });
    }

    /**
     * 批量发送顺序消息（同一业务的多条消息）
     * 适用于批量操作场景
     */
    public void sendBatchOrderedMessages(String businessId, java.util.List<Object> payloads) {
        for (Object payload : payloads) {
            sendOrderedMessage(businessId, payload);
        }
    }
}