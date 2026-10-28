package com.example.kafka.producer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 事务消息生产者
 * 保证本地业务和Kafka消息发送的一致性
 *
 * 场景：订单创建成功后发送消息到Kafka，要求消息要么都成功，要么都失败
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionalProducer {

    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;
    private final KafkaTransactionManager<String, Object> transactionManager;

    @Value("${kafka.topics.transactional}")
    private String transactionalTopic;

    /**
     * 方案一：使用 @Transactional 注解 + KafkaTransactionManager
     * 要求：配置了 KafkaTransactionManager 作为事务管理器
     */
    @Transactional(rollbackFor = Exception.class)
    public void sendWithTransaction(String businessId, Object payload) {
        try {
            // 1. 执行本地业务操作（例如：保存订单到数据库）
            // orderService.saveOrder(order);

            // 2. 发送Kafka消息（事务内）
            Message<Object> message = Message.builder()
                    .businessId(businessId)
                    .payload(payload)
                    .source("transactional-producer")
                    .build();

            transactionalKafkaTemplate.send(transactionalTopic, businessId, message);

            log.info("事务消息发送成功: businessId={}", businessId);

            // 3. 如果这里抛出异常，消息不会提交，本地事务也会回滚
        } catch (Exception e) {
            log.error("事务消息发送失败: businessId={}", businessId, e);
            throw new RuntimeException("事务处理失败", e);
        }
    }

    /**
     * 方案二：编程式事务 + 事务同步器
     * 更灵活的控制方式
     *
     * 适用场景：需要在事务提交后再发送消息
     */
    public void sendAfterTransactionCommit(String businessId, Object payload) {
        // 判断当前是否存在事务
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            // 无事务，直接发送
            sendMessageImmediately(businessId, payload);
            return;
        }

        // 有事务，注册事务同步器，等待事务提交后再发送
        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        // 事务提交后异步发送消息
                        sendMessageImmediately(businessId, payload);
                    }

                    @Override
                    public void afterCompletion(int status) {
                        if (status == TransactionSynchronization.STATUS_ROLLED_BACK) {
                            log.warn("事务已回滚，不发送消息: businessId={}", businessId);
                        }
                    }
                }
        );
    }

    /**
     * 立即发送消息
     */
    private void sendMessageImmediately(String businessId, Object payload) {
        try {
            Message<Object> message = Message.builder()
                    .businessId(businessId)
                    .payload(payload)
                    .source("transactional-producer")
                    .build();

            transactionalKafkaTemplate.send(transactionalTopic, businessId, message);
            log.info("消息已发送: businessId={}", businessId);
        } catch (Exception e) {
            log.error("消息发送失败: businessId={}", businessId, e);
            // 记录失败，由补偿任务处理
        }
    }

    /**
     * 手动开启事务发送（使用 KafkaTransactionManager）
     */
    public void sendWithManualTransaction(String businessId, Object payload) {
        // 开启事务
        org.springframework.transaction.TransactionStatus status =
                transactionManager.getTransaction(new org.springframework.transaction.TransactionDefinition() {
                    @Override
                    public int getPropagationBehavior() {
                        return PROPAGATION_REQUIRED;
                    }
                    // ... 其他配置
                });

        try {
            Message<Object> message = Message.builder()
                    .businessId(businessId)
                    .payload(payload)
                    .build();

            transactionalKafkaTemplate.send(transactionalTopic, businessId, message);

            // 提交事务
            transactionManager.commit(status);
            log.info("事务提交成功: businessId={}", businessId);
        } catch (Exception e) {
            // 回滚事务
            transactionManager.rollback(status);
            log.error("事务回滚: businessId={}", businessId, e);
            throw new RuntimeException("事务发送失败", e);
        }
    }
}