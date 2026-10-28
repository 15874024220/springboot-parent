package com.example.kafka.consumer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 顺序消息消费者
 *
 * 核心原理：
 * 1. Kafka 保证同一分区内消息的有序性
 * 2. 消费者使用单线程处理每个分区
 * 3. 相同业务ID的消息进入同一分区
 *
 * 重要说明：
 * - 顺序消费要求同一业务的消息必须由同一个消费者实例的同一个线程处理
 * - Spring Kafka 中，每个分区会分配一个线程
 * - 消费者并发数不能超过主题分区数
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderedConsumer {

    /**
     * 业务锁管理器：用于保证同一业务的消息按顺序处理
     * 虽然Kafka保证了分区顺序，但在异步处理场景下仍需业务锁
     */
    private final ConcurrentHashMap<String, ReentrantLock> businessLocks = new ConcurrentHashMap<>();

    /**
     * 消费顺序消息
     *
     * 顺序消费的关键配置：
     * 1. 生产者使用相同的KEY发送相关消息
     * 2. 主题有多个分区，但相同KEY进入同一分区
     * 3. 消费者并发数 = 分区数，保证每个分区被单线程处理
     *
     * @param record 消息记录
     * @param ack 手动确认对象
     */
    @KafkaListener(
            topics = "${kafka.topics.ordered}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "3"  // 并发数 = 分区数，确保每个分区被独立线程处理
    )
    public void consume(ConsumerRecord<String, Message<?>> record, Acknowledgment ack) {
        Message<?> message = record.value();
        String businessId = message != null ? message.getBusinessId() : null;
        String key = record.key();  // key 就是业务ID

        log.info("顺序消息消费: businessId={}, key={}, partition={}, offset={}",
                businessId, key, record.partition(), record.offset());

        // 方案一：利用 Kafka 分区保证顺序（推荐）
        // 只要保证相同 businessId 的消息在同一分区，且单线程消费即可
        try {
            processOrderedBusiness(message);
            ack.acknowledge();
            log.info("顺序消息处理成功: businessId={}, offset={}", businessId, record.offset());
        } catch (Exception e) {
            log.error("顺序消息处理失败: businessId={}", businessId, e);
            // 注意：不提交偏移量，消息会重新消费
            // 但要防止死循环，需要记录重试次数
        }

        /*
         * 方案二：使用业务锁（细粒度控制）
         * 适用于消费逻辑中包含异步操作的情况
         */
        // ReentrantLock lock = businessLocks.computeIfAbsent(businessId,
        //     k -> new ReentrantLock());
        // lock.lock();
        // try {
        //     processOrderedBusiness(message);
        //     ack.acknowledge();
        // } finally {
        //     lock.unlock();
        // }
    }

    /**
     * 处理顺序业务
     * 这里必须保证单线程处理，不能使用异步或多线程
     */
    private void processOrderedBusiness(Message<?> message) {
        Object payload = message.getPayload();

        // 场景示例：订单状态流转
        // 必须按顺序处理：创建 -> 支付 -> 发货 -> 完成
        log.info("处理顺序消息: businessId={}, payload={}",
                message.getBusinessId(), payload);

        // TODO: 实际的业务处理
        // 注意：这里不能做异步操作，否则会破坏顺序

        // 模拟处理时间
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}