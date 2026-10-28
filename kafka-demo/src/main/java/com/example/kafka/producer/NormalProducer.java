package com.example.kafka.producer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 普通消息生产者
 * 使用异步发送+回调机制
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NormalProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.normal}")
    private String normalTopic;

    /**
     * 异步发送消息（推荐）
     * 优点：吞吐量高，不阻塞主线程
     * 缺点：无法保证消息顺序
     */
    public void sendAsync(Message<?> message) {
        // 使用消息ID作为key（保证相同ID进入同一分区）
        String key = message.getMessageId();

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(normalTopic, key, message);

        // 添加回调：whenComplete 方法处理成功或失败
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // 成功回调
                log.info("消息发送成功: topic={}, partition={}, offset={}, messageId={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        message.getMessageId());
            } else {
                // 失败回调
                log.error("消息发送失败: messageId={}, error={}",
                        message.getMessageId(), ex.getMessage(), ex);
                saveFailedMessage(message, ex);
            }
        });

        // 注意：future.join() 或 future.get() 可用于同步等待
    }

    /**
     * 同步发送消息（保证顺序的场景使用）
     * 缺点：会阻塞线程，影响性能
     * 适用：对顺序要求极高且吞吐量不高的场景
     */
    public void sendSync(Message<?> message) {
        try {
            SendResult<String, Object> result =
                    kafkaTemplate.send(normalTopic, message.getKey(), message).get();
            log.info("消息同步发送成功: offset={}", result.getRecordMetadata().offset());
        } catch (Exception e) {
            log.error("消息同步发送失败: {}", e.getMessage(), e);
            throw new RuntimeException("消息发送失败", e);
        }
    }

    /**
     * 保存失败消息（生产环境必须实现）
     */
    private void saveFailedMessage(Message<?> message, Throwable ex) {
        // TODO: 保存到失败消息表，由定时任务重试
        log.warn("保存失败消息到数据库: {}", message.getMessageId());
    }
}