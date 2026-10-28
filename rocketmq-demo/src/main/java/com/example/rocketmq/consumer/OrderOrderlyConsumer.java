package com.example.rocketmq.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.constant.MQConstant;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 顺序消息消费者
 *
 * 特点：
 * 1. 使用 ConsumeMode.ORDERLY 保证同一队列的消息顺序消费
 * 2. 同一个订单的状态变更会按顺序处理
 *
 * 注意：
 * - 同一个ConsumerGroup下的所有实例订阅必须完全一致
 * - 顺序消费模式下，消费失败会阻塞后续消息
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "${order.topic}",
        selectorExpression = "${order.orderly-tag}",
        consumerGroup = MQConstant.ORDERLY_CONSUMER_GROUP,
        consumeMode = ConsumeMode.ORDERLY,           // 关键：顺序消费模式
        maxReconsumeTimes = MQConstant.MAX_RECONSUME_TIMES
)
public class OrderOrderlyConsumer implements RocketMQListener<String> {

    private final OrderService orderService;

    /**
     * 顺序消费消息
     *
     * 保证同一个订单的状态变更按顺序处理：
     * 创建 -> 支付 -> 完成
     *
     * 注意：如果消费失败抛出异常，会阻塞同一队列后续消息的消费
     */
    @Override
    public void onMessage(String messageJson) {
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);

        log.info("顺序消费消息 - OrderId: {}, Status: {}",
                messageDTO.getOrderId(), messageDTO.getStatus());

        // 顺序处理订单状态流转
        boolean success = orderService.processOrderStatusSequentially(messageDTO);

        if (!success) {
            log.error("顺序处理失败 - OrderId: {}", messageDTO.getOrderId());
            // 抛出异常会阻塞后续消息，需要确保业务最终成功
            throw new RuntimeException("顺序消息处理失败");
        }

        log.info("顺序消费成功 - OrderId: {}", messageDTO.getOrderId());
    }
}