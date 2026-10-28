package com.example.kafka.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 监控指标收集
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMonitor {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // 监控指标
    private final AtomicLong produceSuccessCount = new AtomicLong(0);
    private final AtomicLong produceFailureCount = new AtomicLong(0);
    private final AtomicLong consumeSuccessCount = new AtomicLong(0);
    private final AtomicLong consumeFailureCount = new AtomicLong(0);

    /**
     * 记录生产成功
     */
    public void recordProduceSuccess() {
        produceSuccessCount.incrementAndGet();
    }

    /**
     * 记录生产失败
     */
    public void recordProduceFailure() {
        produceFailureCount.incrementAndGet();
    }

    /**
     * 记录消费成功
     */
    public void recordConsumeSuccess() {
        consumeSuccessCount.incrementAndGet();
    }

    /**
     * 记录消费失败
     */
    public void recordConsumeFailure() {
        consumeFailureCount.incrementAndGet();
    }

    /**
     * 定时打印监控指标
     */
    @Scheduled(fixedDelay = 60000) // 每分钟输出一次
    public void printMetrics() {
        log.info("===== Kafka 监控指标 =====");
        log.info("生产成功: {}, 生产失败: {}",
                produceSuccessCount.get(), produceFailureCount.get());
        log.info("消费成功: {}, 消费失败: {}",
                consumeSuccessCount.get(), consumeFailureCount.get());
        log.info("成功比例: {}%",
                String.format("%.2f", getSuccessRate(produceSuccessCount.get(), produceFailureCount.get())));
//        log.info("成功比例: {:.2f}%",
//                getSuccessRate(produceSuccessCount.get(), produceFailureCount.get()));
        log.info("=========================");
    }

    private double getSuccessRate(long success, long failure) {
        long total = success + failure;
        return total == 0 ? 100.0 : (success * 100.0 / total);
    }

    /**
     * 健康检查端点
     */
    public boolean isKafkaHealthy() {
        try {
            // 可以发送检查消息或检查连接状态
            return kafkaTemplate.getProducerFactory().createProducer() != null;
        } catch (Exception e) {
            log.error("Kafka健康检查失败", e);
            return false;
        }
    }
}