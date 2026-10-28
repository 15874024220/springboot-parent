package com.example.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka 全局异常处理器
 *
 * 处理场景：
 * 1. 消费者异常
 * 2. 反序列化异常
 * 3. 容器启动异常
 */
@Slf4j
@Component
public class KafkaErrorHandler implements CommonErrorHandler {

    /**
     * 处理其他异常（如 poll 异常、反序列化异常等）
     * 当异常发生时没有具体的 ConsumerRecord 可用时调用
     */
    @Override
    public void handleOtherException(Exception thrownException,
                                     Consumer<?, ?> consumer,
                                     MessageListenerContainer container,
                                     boolean batchListener) {
        log.error("Kafka其他异常: consumer={}, batchListener={}, error={}",
                consumer.groupMetadata().groupId(),
                batchListener,
                thrownException.getMessage(),
                thrownException);

        // 这里可以根据需要决定是否抛出异常
        // 不抛出异常，容器会继续运行
    }

    /**
     * 处理单条记录异常（推荐使用此方法）
     * 返回 true 表示异常已被处理，不会重新提交给监听器
     * 返回 false 表示异常未处理，容器会重新提交记录给监听器
     */
    @Override
    public boolean handleOne(Exception thrownException,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {

        log.error("Kafka单条记录处理异常: topic={}, partition={}, offset={}, error={}",
                record.topic(),
                record.partition(),
                record.offset(),
                thrownException.getMessage(),
                thrownException);

        // 判断是否为不可恢复异常
        if (isNonRecoverableException(thrownException)) {
            log.warn("不可恢复异常，消息将被跳过: topic={}, offset={}",
                    record.topic(), record.offset());
            // TODO: 记录到死信表或其他持久化存储
            // 返回 true 表示已处理，不会重新提交
            return true;
        }

        // 可恢复异常，返回 false 让框架重新尝试处理
        return false;
    }

    /**
     * 处理剩余记录异常（当 seeksAfterHandling 返回 true 时使用）
     * 通常用于批量处理失败后，seek 剩余记录以便重新消费
     */
    @Override
    public void handleRemaining(Exception thrownException,
                                List<ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer,
                                MessageListenerContainer container) {

        log.error("Kafka剩余记录处理异常: recordsCount={}, error={}",
                records.size(),
                thrownException.getMessage(),
                thrownException);

        // 对剩余记录进行 seek 操作，以便重新消费
        records.forEach(record -> {
            consumer.seek(
                    new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()),
                    record.offset()
            );
            log.debug("Seek to offset: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
        });
    }

    /**
     * 判断是否为不可恢复异常
     */
    private boolean isNonRecoverableException(Exception e) {
        // 反序列化异常 - 无法恢复
        if (e instanceof org.apache.kafka.common.errors.SerializationException) {
            return true;
        }

        // 参数校验异常 - 无法恢复
        if (e instanceof IllegalArgumentException) {
            return true;
        }

        // 消息体过大异常 - 无法恢复
        if (e instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
            return true;
        }

        return false;
    }

    /**
     * 当异常处理后，是否应该提交 offset
     * 返回 true 表示会提交已处理记录的 offset（默认）
     * 返回 false 表示不提交
     */
    @Override
    public boolean isAckAfterHandle() {
        // 对于不可恢复异常，我们返回了 true 表示已处理，offset 会被提交
        // 这样可以避免无限重试
        return true;
    }

    /**
     * 设置异常处理后是否提交 offset
     * 如果需要动态控制，可以实现此方法
     */
    @Override
    public void setAckAfterHandle(boolean ack) {
        // 如果需要动态修改配置，可以在这里实现
        // 当前使用默认实现，不需要特殊处理
    }
}