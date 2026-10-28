package com.example.kafka.consumer;

import com.example.kafka.model.Message;
import com.example.kafka.service.IdempotentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 普通消息消费者
 *
 * 核心功能：
 * 1. 手动提交偏移量
 * 2. 幂等处理（防止重复消费）
 * 3. 异常处理
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NormalConsumer {

    private final IdempotentService idempotentService;

    @Value("${kafka.topics.normal}")
    private String normalTopic;

    /**
     * 消费普通消息
     *
     * 重复消费产生原因：
     * 1. 消费者处理超时，触发 Rebalance，重新分配分区后重新消费
     * 2. 手动提交偏移量失败
     * 3. 消费者重启时偏移量未及时提交
     *
     * 解决方案：
     * 1. 幂等处理（使用 messageId 或 businessId 去重）
     * 2. 合理配置 max.poll.interval.ms
     * 3. 手动提交偏移量
     *
     * @param record 消息记录
     * @param ack 手动确认对象
     */
    @KafkaListener(
            topics = "${kafka.topics.normal}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Message<?>> record, Acknowledgment ack) {
        Message<?> message = null;
        try {
            message = record.value();
            log.info("收到消息: topic={}, partition={}, offset={}, messageId={}, businessId={}",
                    record.topic(), record.partition(), record.offset(),
                    message != null ? message.getMessageId() : null,
                    message != null ? message.getBusinessId() : null);

            if (message == null) {
                log.warn("消息内容为空，offset={}", record.offset());
                ack.acknowledge(); // 确认消费
                return;
            }

            // ========== 幂等处理：防止重复消费 ==========
            // 使用 messageId 作为唯一标识，检查是否已处理过
            String messageId = message.getMessageId();
            if (idempotentService.isProcessed(messageId)) {
                log.warn("消息已处理过，跳过重复消费: messageId={}", messageId);
                ack.acknowledge(); // 确认消费，避免无限重试
                return;
            }

            // ========== 业务处理 ==========
            processBusiness(message);

            // ========== 记录处理成功（幂等标记） ==========
            idempotentService.markProcessed(messageId);

            // ========== 手动提交偏移量 ==========
            // 重要：在业务处理成功后才提交偏移量
            ack.acknowledge();
            log.info("消息消费成功并已提交偏移量: messageId={}", messageId);

        } catch (IllegalArgumentException e) {
            log.error("消息消费失败: offset={}, message={}",
                    record.offset(), message, e);

            // 异常处理策略：
            // 1. 对于可恢复的异常（如网络超时），不提交偏移量，消息会重新消费
            // 2. 对于不可恢复的异常（如数据校验失败），可能需要记录到死信队列

            if (isRecoverableException(e)) {
                // 可恢复异常：不提交偏移量，让Kafka重新投递
                log.warn("可恢复异常，消息将重新消费");
                // 注意：这里不调用 ack.acknowledge()
            } else {
                // 不可恢复异常：记录错误，手动提交避免阻塞
                log.error("不可恢复异常，记录到死信表后提交偏移量");
                // TODO: 发送到死信队列或记录到失败表
                ack.acknowledge();
            }

            // 业务异常不提交偏移量，让消息重新被消费
            // 注意：需要配置重试次数，避免无限重试

            // 不可重试异常：不重试，但仍然会通过 DeadLetterPublishingRecoverer 进入死信
            log.error("不可重试异常，消息将进入死信队列: {}", e.getMessage());
            // 重新抛出，让 ErrorHandler 处理
            throw e;
        } catch (RuntimeException e) {
            // 可重试异常：重试3次后进入死信
            log.error("业务处理异常，将进行重试: {}", e.getMessage());
            throw new RuntimeException("处理失败", e);
        }
    }

    /**
     * 判断异常是否可恢复
     */
    private boolean isRecoverableException(Exception e) {
        // 可根据异常类型判断
        // 网络异常、超时异常 => 可恢复
        // 数据格式错误、业务逻辑错误 => 不可恢复
        String message = e.getMessage();
        return message != null && (
                message.contains("timeout") ||
                        message.contains("connection") ||
                        message.contains("network")
        );
    }

    /**
     * 业务处理逻辑（示例）
     */
    private void processBusiness(Message<?> message) {
        Object payload = message.getPayload();
        log.info("处理业务数据: {}", payload);

        // 模拟业务处理
        // TODO: 实际业务逻辑
        if (payload == null) {
            throw new IllegalArgumentException("业务数据不能为空");
        }

        // 模拟可能的异常
        // if (someCondition) {
        //     throw new RuntimeException("业务处理失败");
        // }

        // ========== 模拟可能导致死信的场景 ==========

        // 场景1：业务数据校验失败（不可重试异常 -> 直接进死信）
        if (payload == null) {
            log.error("消息内容为空，这是不可恢复的错误，直接进入死信队列");
            throw new IllegalArgumentException("消息内容不能为空");
        }

        // 场景2：模拟业务处理失败（可重试异常 -> 重试3次后进死信）
        if (message.getRetryCount() != null && message.getRetryCount() > 0) {
            log.warn("重试处理: retryCount={}", message.getRetryCount());
        }

        // 模拟处理逻辑 - 这里可以设置条件让消息失败
        boolean shouldFail = processBusinessLogic(message);

        if (shouldFail) {
            // 抛出异常，触发重试机制
            throw new RuntimeException("业务处理失败，将触发重试");
        }
    }

    /**
     * 模拟业务逻辑
     * @return true-处理失败，false-处理成功
     */
    private boolean processBusinessLogic(Message<?> message) {
        Object payload = message.getPayload();

        // 示例：如果消息内容是 "error"，则处理失败
        if (payload instanceof String && "error".equals(payload)) {
            log.warn("检测到错误标记，模拟处理失败");
            return true;
        }

        // 示例：如果消息内容是 "timeout"，模拟超时
        if (payload instanceof String && "timeout".equals(payload)) {
            log.warn("模拟超时场景");
            try {
                Thread.sleep(5000); // 模拟长时间处理
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        }

        // 正常处理
        log.info("正常处理业务逻辑: payload={}", payload);
        return false;
    }
}