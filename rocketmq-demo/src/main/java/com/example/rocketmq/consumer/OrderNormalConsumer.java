package com.example.rocketmq.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.constant.MQConstant;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.handler.MqConsumerErrorHandler;
import com.example.rocketmq.service.MessageIdempotentService;
import com.example.rocketmq.service.OrderService;
import com.example.rocketmq.util.TraceIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 普通消息消费者
 *
 * 功能：
 * 1. 消费订单消息
 * 2. 支持重试机制
 * 3. 支持幂等性校验
 * 4. 消费失败自动重试，超过重试次数进入死信队列
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "${order.topic}",                    // 订阅的Topic
        selectorExpression = "${order.normal-tag}",  // 订阅的Tag
        consumerGroup = MQConstant.NORMAL_CONSUMER_GROUP,  // 消费者组名
        maxReconsumeTimes = MQConstant.MAX_RECONSUME_TIMES, // 最大重试次数，超过进死信队列
        consumeThreadNumber = 20,                    // 消费线程数
        consumeTimeout = 15                          // 消费超时时间（分钟）
)
public class OrderNormalConsumer implements RocketMQListener<String> {

    private final OrderService orderService;
    private final MessageIdempotentService idempotentService;
    private final MqConsumerErrorHandler errorHandler;  // 注入错误处理器

    /**
     * 消费消息
     *
     * 消费流程：
     * 1. 解析消息
     * 2. 幂等性校验（防止重复消费）
     * 3. 业务处理
     * 4. 标记已处理
     *
     * 注意：
     * - 方法抛出异常时，消息会重新入队重试
     * - 重试次数超过maxReconsumeTimes后，进入死信队列
     */
    @Override
    public void onMessage(String messageJson) {
        // 解析消息
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);
        String orderId = messageDTO.getOrderId();
        String messageId = messageDTO.getMessageId();
        Integer retryTimes = messageDTO.getRetryTimes() != null ? messageDTO.getRetryTimes() : 0;

        // 设置链路追踪ID
        TraceIdUtil.setTraceId(messageId);

        log.info("接收消息 - OrderId: {}, MessageId: {}, RetryTimes: {}",
                orderId, messageId, messageDTO.getRetryTimes());

        try {
            // 1. 幂等性校验（防止重复消费）
            // 如果消息已经处理过，直接返回成功
            if (idempotentService.isProcessed(messageId, orderId)) {
                log.warn("消息已处理过，直接ACK - OrderId: {}, MessageId: {}", orderId, messageId);
                return;
            }

            // 2. 业务处理
            boolean success = orderService.processOrderMessage(messageDTO);

            if (!success) {
                log.error("业务处理失败 - OrderId: {}", orderId);
                throw new RuntimeException("业务处理失败，需要重试");
            }

            // 3. 标记消息已处理（保存到Redis，设置过期时间）
            idempotentService.markProcessed(messageId, orderId);

            log.info("消息消费成功 - OrderId: {}, MessageId: {}", orderId, messageId);

        } catch (Exception e) {
            log.error("消息消费异常 - OrderId: {}, MessageId: {}", orderId, messageId, e);

            // 调用错误处理器，决定是否重试
            boolean shouldRetry = errorHandler.handleError(
                    messageJson, orderId, e, retryTimes
            );

            if (shouldRetry) {
                // 更新重试次数后抛出异常，让 RocketMQ 重试
                messageDTO.setRetryTimes(retryTimes + 1);
                throw new RuntimeException(e);
            } else {
                // 不需要重试，正常返回（ACK）
                log.warn("消费失败且不重试 - OrderId: {}", orderId);
            }
        } finally {
            TraceIdUtil.clearTraceId();
        }
    }
}