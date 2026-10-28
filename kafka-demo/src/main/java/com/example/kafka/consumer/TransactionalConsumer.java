package com.example.kafka.consumer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 事务消息消费者
 *
 * 配合生产者的事务消息，实现端到端的一致性
 * 使用隔离级别: read_committed 只能消费已提交的事务消息
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionalConsumer {

    /**
     * 消费事务消息
     *
     * 注意：
     * 1. 消费者配置 isolation-level: read_committed
     * 2. 只能消费生产者已提交的事务消息
     * 3. 未提交的事务消息不会被消费
     */
    @KafkaListener(
            topics = "${kafka.topics.transactional}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional(rollbackFor = Exception.class)
    public void consume(ConsumerRecord<String, Message<?>> record, Acknowledgment ack) {
        Message<?> message = record.value();

        log.info("消费事务消息: businessId={}, offset={}, partition={}",
                message != null ? message.getBusinessId() : null,
                record.offset(), record.partition());

        try {
            // 业务处理（需要支持回滚）
            processTransactionalBusiness(message);

            // 确认消费
            ack.acknowledge();
            log.info("事务消息消费成功");

        } catch (Exception e) {
            log.error("事务消息消费失败，将回滚本地事务", e);
            // 抛出异常，Spring 会回滚本地事务
            // Kafka 偏移量不会提交，消息会重新消费
            throw new RuntimeException("事务处理失败", e);
        }
    }

    /**
     * 事务性业务处理
     * 这里的所有操作都会在一个数据库事务中
     */
    private void processTransactionalBusiness(Message<?> message) {
        Object payload = message.getPayload();

        // 1. 更新数据库状态
        // orderService.updateOrderStatus(...);

        // 2. 其他业务操作

        log.info("事务性业务处理完成: {}", payload);
    }
}