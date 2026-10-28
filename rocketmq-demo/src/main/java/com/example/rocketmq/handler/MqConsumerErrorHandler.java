package com.example.rocketmq.handler;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.domain.entity.MessageConsumeLog;
import com.example.rocketmq.mapper.MessageLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * RocketMQ 消息消费错误处理器
 *
 * 职责：
 * 1. 统一处理消费过程中的各类异常
 * 2. 根据异常类型智能判断是否重试
 * 3. 记录消费失败日志到数据库
 * 4. 超限失败时保存到死信表
 * 5. 发送告警通知（钉钉/邮件）
 *
 * @author example
 * @date 2024
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqConsumerErrorHandler {

    private final MessageLogMapper messageLogMapper;

    // 最大重试次数（与注解中的 maxReconsumeTimes 保持一致）
    private static final int MAX_RETRY_TIMES = 3;

    // 是否发送告警
    private static final boolean ENABLE_ALERT = true;

    /**
     * 处理消费异常（主方法）
     *
     * @param message 原始消息内容
     * @param businessKey 业务Key（如订单号）
     * @param exception 发生的异常
     * @param retryTimes 当前重试次数（从消息体中获取）
     * @return true-需要重试，false-不需要重试（直接ACK或进死信）
     */
    public boolean handleError(String message, String businessKey,
                               Exception exception, int retryTimes) {

        log.error("消息消费异常 - BusinessKey: {}, RetryTimes: {}, Exception: {}",
                businessKey, retryTimes, exception.getMessage(), exception);

        // 1. 记录失败日志到数据库
        saveConsumeLog(message, businessKey, exception, retryTimes);

        // 2. 根据异常类型判断是否可重试
        boolean shouldRetry = shouldRetry(exception);

        if (!shouldRetry) {
            log.warn("不可重试异常，直接丢弃 - BusinessKey: {}, Exception: {}",
                    businessKey, exception.getClass().getSimpleName());
            return false;
        }

        // 3. 判断是否超过最大重试次数
        if (retryTimes >= MAX_RETRY_TIMES) {
            log.error("超过最大重试次数 - BusinessKey: {}, RetryTimes: {}", businessKey, retryTimes);

            // 保存到死信表
            saveToDeadLetter(message, businessKey, exception);

            // 发送告警
            if (ENABLE_ALERT) {
                sendAlert(businessKey, exception, retryTimes);
            }

            return false;  // 不再重试，让消息进入死信队列
        }

        // 4. 需要重试
        log.info("消息将进行重试 - BusinessKey: {}, 下次重试次数: {}", businessKey, retryTimes + 1);
        return true;
    }

    /**
     * 判断是否需要重试
     *
     * 策略：
     * - 业务异常（参数错误、数据不存在等）：不重试
     * - 临时异常（网络超时、连接失败等）：重试
     * - 系统异常：重试
     */
    private boolean shouldRetry(Exception exception) {
        String exceptionName = exception.getClass().getSimpleName();
        String message = exception.getMessage();

        // 1. 参数校验异常 - 不重试
        if (exception instanceof IllegalArgumentException) {
            return false;
        }

        // 2. 业务异常 - 不重试
        if (exceptionName.contains("BusinessException")) {
            return false;
        }

        // 3. 数据不存在 - 不重试
        if (message != null && message.contains("不存在")) {
            return false;
        }

        // 4. 网络超时 - 重试
        if (message != null && (message.contains("timeout") || message.contains("超时"))) {
            return true;
        }

        // 5. 连接异常 - 重试
        if (message != null && (message.contains("connection") || message.contains("连接"))) {
            return true;
        }

        // 6. 数据库异常 - 重试
        if (exceptionName.contains("SQLException") || message != null && message.contains("数据库")) {
            return true;
        }

        // 7. 其他异常默认重试
        return true;
    }

    /**
     * 保存消费日志到数据库
     */
    private void saveConsumeLog(String message, String businessKey,
                                Exception exception, int retryTimes) {
        try {
            // 解析消息获取更多信息
            OrderMessageDTO messageDTO = null;
            String messageId = null;
            try {
                messageDTO = JSON.parseObject(message, OrderMessageDTO.class);
                messageId = messageDTO != null ? messageDTO.getMessageId() : null;
            } catch (Exception e) {
                log.warn("解析消息失败，无法获取 messageId");
            }

            MessageConsumeLog consumeLog = MessageConsumeLog.builder()
                    .messageId(messageId)
                    .businessKey(businessKey)
                    .consumerGroup("normal_consumer_group")
                    .status(2)  // 2-失败
                    .retryTimes(retryTimes)
                    .errorMessage(exception.getMessage())
                    .createTime(LocalDateTime.now())
                    .build();

            messageLogMapper.insert(consumeLog);
            log.debug("消费失败日志已保存 - BusinessKey: {}", businessKey);

        } catch (Exception e) {
            log.error("保存消费日志失败", e);
        }
    }

    /**
     * 保存到死信表
     */
    private void saveToDeadLetter(String message, String businessKey, Exception exception) {
        try {
            log.warn("保存死信消息 - BusinessKey: {}, Error: {}", businessKey, exception.getMessage());

            // TODO: 保存到专门的死信表
            // deadLetterMapper.insert(DeadLetter.builder()
            //         .messageId(messageId)
            //         .businessKey(businessKey)
            //         .messageContent(message)
            //         .errorMessage(exception.getMessage())
            //         .status(0)  // 0-待处理
            //         .createTime(LocalDateTime.now())
            //         .build());

        } catch (Exception e) {
            log.error("保存死信消息失败", e);
        }
    }

    /**
     * 发送告警通知
     */
    private void sendAlert(String businessKey, Exception exception, int retryTimes) {
        log.warn("发送消费失败告警 - BusinessKey: {}, RetryTimes: {}", businessKey, retryTimes);

        // TODO: 实现告警逻辑（钉钉、邮件、企微等）
        // String alertMsg = String.format(
        //     "【RocketMQ告警】\n订单号: %s\n重试次数: %d\n异常信息: %s\n时间: %s",
        //     businessKey, retryTimes, exception.getMessage(), DateUtil.now()
        // );
        // dingTalkService.send(alertMsg);
        // emailService.send(alertMsg);
    }
}