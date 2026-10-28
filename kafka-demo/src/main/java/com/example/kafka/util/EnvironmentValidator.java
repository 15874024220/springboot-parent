package com.example.kafka.util;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Kafka + Redis 环境验证工具
 * 用于检测 Docker 环境是否正常
 */
@Slf4j
@Component
public class EnvironmentValidator {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired(required = false)
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @PostConstruct
    public void validate() {
        log.info("========== 环境验证开始 ==========");

        // 1. 验证 Kafka 连接
        validateKafkaConnection();

        // 2. 验证 Redis 连接
        validateRedisConnection();

        log.info("========== 环境验证结束 ==========");
    }

    /**
     * 验证 Kafka 连接
     */
    private void validateKafkaConnection() {
        log.info("验证 Kafka 连接: {}", bootstrapServers);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 尝试列出主题
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true);

            Set<String> topics = adminClient.listTopics(options)
                    .names()
                    .get();

            log.info("✅ Kafka 连接成功！已存在的主题: {}", topics);

            if (topics.isEmpty()) {
                log.warn("⚠️ 当前没有创建任何主题，启动后会自动创建");
            }

        } catch (InterruptedException | ExecutionException e) {
            log.error("❌ Kafka 连接失败！请检查：");
            log.error("   1. Docker 容器是否启动: docker ps | grep kafka");
            log.error("   2. Kafka 端口映射是否正确: 9092->9092");
            log.error("   3. Kafka 是否健康: docker logs kafka-broker");
            log.error("   错误详情: {}", e.getMessage());
        }
    }

    /**
     * 验证 Redis 连接
     */
    private void validateRedisConnection() {
        if (redisTemplate == null) {
            log.warn("⚠️ RedisTemplate 未配置，跳过 Redis 验证");
            return;
        }

        try {
            String testKey = "test:connection:" + System.currentTimeMillis();
            redisTemplate.opsForValue().set(testKey, "ok");
            String value = (String) redisTemplate.opsForValue().get(testKey);
            redisTemplate.delete(testKey);

            if ("ok".equals(value)) {
                log.info("✅ Redis 连接成功！");
            } else {
                log.error("❌ Redis 连接失败：读写测试不通过");
            }
        } catch (Exception e) {
            log.error("❌ Redis 连接失败！请检查：");
            log.error("   1. Redis 容器是否启动: docker ps | grep redis");
            log.error("   2. Redis 端口映射是否正确: 6379->6379");
            log.error("   错误详情: {}", e.getMessage());
        }
    }
}