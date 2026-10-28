package com.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka核心配置类（适配 M1 Docker 环境）
 * 包含生产者、消费者、监听器容器工厂的配置
 *
 * 注意：由于 Kafka 在 Docker 中运行，需要特别注意网络配置
 *  * - 容器内使用 kafka:19092 通信
 *  * - 容器外（Spring Boot）使用 localhost:9092 连接
 * @author demo
 */
@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.dead-letter}")
    private String deadLetterTopic;

    @Value("${kafka.topics.normal}")
    private String normalTopic;

    @Value("${kafka.topics.ordered}")
    private String orderedTopic;

    @Value("${kafka.topics.transactional}")
    private String transactionalTopic;

    // ==================== 生产者配置 ====================

    /**
     * 普通生产者配置（非事务）
     */
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // 重试配置
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        // 批量发送配置
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // 压缩配置
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 幂等性和确认机制
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 最大请求大小
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);

        // M1 Docker 环境优化：增加连接超时时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return props;
    }

    /**
     * 事务生产者配置
     * 关键：设置 transactional.id 前缀
     */
    @Bean
    public Map<String, Object> transactionalProducerConfigs() {
        Map<String, Object> props = producerConfigs();
        // 启用事务支持
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-producer-");
        // 确保幂等性开启（事务要求）
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    /**
     * 普通生产者工厂（非事务）
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * 事务生产者工厂
     * 关键：需要调用 setTransactionIdPrefix
     */
    @Bean
    public ProducerFactory<String, Object> transactionalProducerFactory() {
        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(transactionalProducerConfigs());
        // 设置事务ID前缀
        factory.setTransactionIdPrefix("tx-");
        return factory;
    }

    /**
     * 普通 KafkaTemplate
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * 事务 KafkaTemplate
     * 使用事务生产者工厂
     */
    @Bean
    public KafkaTemplate<String, Object> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    // ==================== 核心：KafkaTransactionManager Bean ====================

    /**
     * Kafka 事务管理器
     * 这是解决报错的关键 Bean！
     *
     * 作用：
     * 1. 管理 Kafka 事务
     * 2. 与 @Transactional 注解配合使用
     * 3. 保证本地数据库事务和 Kafka 消息的一致性
     */
    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        // 使用事务生产者工厂创建事务管理器
        KafkaTransactionManager<String, Object> transactionManager =
                new KafkaTransactionManager<>(transactionalProducerFactory());

        // 可选：设置事务超时时间（毫秒）
        transactionManager.setDefaultTimeout(60000);

        log.info("KafkaTransactionManager 初始化成功");
        return transactionManager;
    }

    // ==================== 消费者配置 ====================

    /**
     * 普通消费者配置
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 重要：关闭自动提交，改为手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 反序列化器配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // 拉取配置
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        // M1 Docker 环境优化：连接超时配置
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        // 重要：消费者配置隔离级别为 read_committed（只能消费已提交的事务消息）
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return props;
    }

    /**
     * 带错误处理的消费者容器工厂
     * 配置死信队列和重试机制
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // 设置并发数（根据分区数配置）。 M1 Docker 环境：并发数建议不要超过 CPU 核心数
        factory.setConcurrency(3);

        // 设置手动提交模式
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 配置错误处理器
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                // 死信队列恢复器：发送失败的消息转发到死信主题
                new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, exception) -> new TopicPartition(deadLetterTopic, record.partition())),
                // 重试策略：重试3次，每次间隔1秒
                new FixedBackOff(1000L, 3L)
        );

        // 设置不重试的异常类型（遇到这些异常直接进入死信队列，不重试）
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,      // 参数错误
                NullPointerException.class,           // 空指针
                IllegalStateException.class           // 非法状态
        );
        // 设置可重试的异常类型（默认所有异常都可重试，这里可以精确控制）
        // errorHandler.addRetryableExceptions(TimeoutException.class, NetworkException.class);

        // 设置重试时是否重置状态（默认 false）
        errorHandler.setResetStateOnExceptionChange(false);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    /**
     * 消费者工厂
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    // ==================== 主题配置 ====================

    /**
     * 创建普通主题（1个分区）
     */
    @Bean
    public NewTopic normalTopic() {
        return new NewTopic(normalTopic, 1, (short) 1);
    }

    /**
     * 创建顺序消息主题（指定分区数，确保顺序的关键）
     * 顺序消息原理：同一分区内的消息是有序的
     */
    @Bean
    public NewTopic orderedTopic() {
        return new NewTopic(orderedTopic, 3, (short) 1);
    }

    /**
     * 创建事务消息主题
     */
    @Bean
    public NewTopic transactionalTopic() {
        return new NewTopic(transactionalTopic, 3, (short) 1);
    }

    /**
     * 创建死信队列主题
     */
    @Bean
    public NewTopic deadLetterTopic() {
        return new NewTopic(deadLetterTopic, 3, (short) 1);
    }
}