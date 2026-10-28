package com.example.rocketmq.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.constant.MQConstant;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.service.MessageIdempotentService;
import com.example.rocketmq.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 事务消息消费者
 *
 * 消费由事务消息提交后投递的消息
 *
 * 事务消息流程：
 * 1. 发送半消息
 * 2. 执行本地事务
 * 3. 本地事务成功则提交消息，消费者可见
 * 4. 本地事务失败则回滚消息
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "${order.topic}",
        selectorExpression = "${order.transaction-tag}",
        consumerGroup = MQConstant.TRANSACTION_CONSUMER_GROUP,
        maxReconsumeTimes = MQConstant.MAX_RECONSUME_TIMES
)
public class TransactionConsumer implements RocketMQListener<String> {

    private final OrderService orderService;
    private final MessageIdempotentService idempotentService;

    @Override
    public void onMessage(String messageJson) {
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);
        String orderId = messageDTO.getOrderId();
        String messageId = messageDTO.getMessageId();

        log.info("事务消息消费 - OrderId: {}, MessageId: {}", orderId, messageId);

        try {
            // 幂等性校验
            if (idempotentService.isProcessed(messageId, orderId)) {
                log.warn("事务消息已处理过 - OrderId: {}", orderId);
                return;
            }

            // 处理业务逻辑
            // 注意：事务消息的订单已经在本地事务中创建，这里只需要处理后续业务
            boolean success = orderService.processOrderMessage(messageDTO);

            if (success) {
                idempotentService.markProcessed(messageId, orderId);
                log.info("事务消息消费成功 - OrderId: {}", orderId);
            } else {
                throw new RuntimeException("事务消息处理失败");
            }

        } catch (Exception e) {
            log.error("事务消息消费异常 - OrderId: {}", orderId, e);
            throw new RuntimeException(e);
        }
    }
}