package com.example.rocketmq.handler;

import com.example.rocketmq.mapper.MessageLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 支持延迟重试的错误处理器
 */
@Slf4j
@Component
public class AdvancedMqConsumerErrorHandler extends MqConsumerErrorHandler {

    private final RedisTemplate<String, String> redisTemplate;

    // 重试延迟配置（秒）
    private static final int[] DELAY_SECONDS = {5, 10, 30, 60, 120};

    public AdvancedMqConsumerErrorHandler(RedisTemplate<String, String> redisTemplate, MessageLogMapper essageLogMapper) {
        super(essageLogMapper);
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean handleError(String message, String businessKey,
                               Exception exception, int retryTimes) {

        // 先调用父类的基础处理
        boolean shouldRetry = super.handleError(message, businessKey, exception, retryTimes);

        if (shouldRetry && retryTimes < DELAY_SECONDS.length) {
            // 延迟重试：将消息暂存，延迟后重新投递
            int delaySeconds = DELAY_SECONDS[retryTimes];
            scheduleRetry(message, businessKey, delaySeconds);
            return false;  // 不立即重试，而是延迟后重试
        }

        return shouldRetry;
    }

    /**
     * 延迟重试：将消息存入 Redis，由定时任务重新发送
     */
    private void scheduleRetry(String message, String businessKey, int delaySeconds) {
        String key = "retry:message:" + businessKey;
        redisTemplate.opsForValue().set(key, message, delaySeconds, TimeUnit.SECONDS);
        log.info("消息将延迟{}秒后重试 - BusinessKey: {}", delaySeconds, businessKey);
    }
}