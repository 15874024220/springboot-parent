package com.example.kafka.service;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeadLetterService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.normal}")
    private String normalTopic;

    // 模拟数据库存储（实际应该用真实的数据库）
    private final ConcurrentHashMap<String, DeadLetterRecord> deadLetterStore = new ConcurrentHashMap<>();

    /**
     * 保存死信消息到数据库
     */
    public void saveToDatabase(Message<Object> message, ConsumerRecord<?, ?> record) {
        DeadLetterRecord deadRecord = DeadLetterRecord.builder()
                .messageId(message != null ? message.getMessageId() : null)
                .businessId(message != null ? message.getBusinessId() : null)
                .payload(message != null ? message.getPayload() : null)
                .originalTopic(record.topic())
                .originalPartition(record.partition())
                .originalOffset(record.offset())
                .deadLetterTime(LocalDateTime.now())
                .status("PENDING")  // PENDING: 待处理, PROCESSING: 处理中, RESOLVED: 已解决
                .build();

        deadLetterStore.put(deadRecord.getMessageId(), deadRecord);
        log.info("死信消息已保存到数据库: messageId={}", deadRecord.getMessageId());
    }

    /**
     * 重新发送死信消息到原主题
     */
    public void retryToOriginalTopic(Message<Object> message) {
        if (message == null) {
            log.warn("消息为空，无法重试");
            return;
        }

        // 增加重试次数
        int retryCount = message.getRetryCount() != null ? message.getRetryCount() + 1 : 1;
        message.setRetryCount(retryCount);

        // 重新发送到原主题
        kafkaTemplate.send(normalTopic, message.getBusinessId(), message);
        log.info("死信消息已重新发送到原主题: businessId={}, retryCount={}",
                message.getBusinessId(), retryCount);
    }

    /**
     * 人工处理死信消息
     */
    public void manualResolve(String messageId, Object correctedPayload) {
        DeadLetterRecord record = deadLetterStore.get(messageId);
        if (record == null) {
            log.warn("死信记录不存在: messageId={}", messageId);
            return;
        }

        // 构建修正后的消息
        Message<Object> correctedMessage = Message.builder()
                .messageId(messageId)
                .businessId(record.getBusinessId())
                .payload(correctedPayload)
                .retryCount(0)
                .source("manual-resolve")
                .timestamp(LocalDateTime.now())
                .build();

        // 发送修正后的消息
        kafkaTemplate.send(normalTopic, correctedMessage.getBusinessId(), correctedMessage);

        // 更新状态
        record.setStatus("RESOLVED");
        record.setResolveTime(LocalDateTime.now());

        log.info("死信消息已人工处理: messageId={}", messageId);
    }

    /**
     * 查询死信记录
     */
    public DeadLetterRecord getDeadLetter(String messageId) {
        return deadLetterStore.get(messageId);
    }

    /**
     * 死信记录实体
     */
    @lombok.Data
    @lombok.Builder
    public static class DeadLetterRecord {
        private String messageId;
        private String businessId;
        private Object payload;
        private String originalTopic;
        private Integer originalPartition;
        private Long originalOffset;
        private LocalDateTime deadLetterTime;
        private String status;
        private LocalDateTime resolveTime;
    }
}