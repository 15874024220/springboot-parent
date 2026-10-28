package com.example.kafka.consumer;

import com.example.kafka.model.Message;
import com.example.kafka.service.DeadLetterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 死信队列消费者
 *
 * 职责：
 * 1. 消费失败进入死信队列的消息
 * 2. 记录失败原因，便于排查问题
 * 3. 可以重试、告警或人工介入处理
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final DeadLetterService deadLetterService;

    @Value("${kafka.topics.dead-letter}")
    private String deadLetterTopic;

    /**
     * 消费死信队列的消息
     *
     * 死信消息的特点：
     * 1. 消息头中包含原始信息（原始主题、分区、偏移量）
     * 2. 消息体保持不变
     * 3. 可以获取失败原因（需要配置）
     */
    @KafkaListener(
            topics = "${kafka.topics.dead-letter}",
            groupId = "dead-letter-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDeadLetter(ConsumerRecord<String, Message<Object>> record, Acknowledgment ack) {
        Message<Object> message = record.value();

        log.error("========== 收到死信消息 ==========");
        log.error("死信主题: {}", record.topic());
        log.error("死信分区: {}", record.partition());
        log.error("死信偏移量: {}", record.offset());
        log.error("原始业务ID: {}", message != null ? message.getBusinessId() : null);
        log.error("原始消息ID: {}", message != null ? message.getMessageId() : null);
        log.error("消息内容: {}", message != null ? message.getPayload() : null);
        log.error("消息时间戳: {}", message != null ? message.getTimestamp() : null);

        try {
            // ========== 死信消息处理策略 ==========

            // 策略1：记录到数据库，供人工处理
            deadLetterService.saveToDatabase(message, record);

            // 策略2：发送告警通知（邮件、钉钉、企业微信）
            // alertService.sendAlert("Kafka死信告警", message);

            // 策略3：根据业务ID判断是否需要重试
            if (shouldRetry(message)) {
                log.info("死信消息满足重试条件，将重新发送到原主题");
                deadLetterService.retryToOriginalTopic(message);
            } else {
                log.info("死信消息不满足重试条件，保存到死信表等待人工处理");
            }

            // 策略4：记录失败指标，用于监控
            // metricsCollector.recordDeadLetter(message);

            // 确认消费死信消息
            ack.acknowledge();
            log.info("死信消息处理完成: messageId={}", message != null ? message.getMessageId() : null);

        } catch (Exception e) {
            log.error("处理死信消息失败: {}", e.getMessage(), e);
            // 注意：这里不确认消费，消息会留在死信队列，但可能导致无限循环
            // 生产环境应该记录到专门的错误表，然后确认消费
            // 为了避免无限循环，这里选择确认消费，但记录到本地错误日志
            ack.acknowledge();
            log.error("死信消息处理失败，已确认消费但记录到错误日志");
        }
    }

    /**
     * 判断死信消息是否应该重试
     */
    private boolean shouldRetry(Message<Object> message) {
        if (message == null || message.getRetryCount() == null) {
            return false;
        }
        // 重试次数不超过3次
        return message.getRetryCount() < 3;
    }
}