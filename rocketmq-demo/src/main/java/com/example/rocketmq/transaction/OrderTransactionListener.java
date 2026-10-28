package com.example.rocketmq.transaction;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.domain.entity.Order;
import com.example.rocketmq.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * 事务消息监听器
 *
 * 实现分布式事务的最终一致性
 *
 * 事务消息流程：
 * 1. 发送半消息（prepared message）
 * 2. RocketMQ回调 executeLocalTransaction 执行本地事务
 * 3. 根据本地事务结果返回 COMMIT/ROLLBACK
 * 4. 如果本地事务执行超时或返回UNKNOWN，RocketMQ会回调 checkLocalTransaction 进行回查
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQTransactionListener
public class OrderTransactionListener implements RocketMQLocalTransactionListener {

    private final OrderService orderService;

    /**
     * 执行本地事务（半消息发送成功后回调）
     *
     * @param msg 半消息
     * @param arg 透传的业务参数
     * @return 事务状态：COMMIT（提交）/ ROLLBACK（回滚）/ UNKNOWN（未知，等待回查）
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String orderId = (String) arg;
        String messageJson = new String((byte[]) msg.getPayload());
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);

        log.info("执行本地事务 - OrderId: {}, MessageId: {}",
                orderId, messageDTO.getMessageId());

        try {
            // 创建订单（本地数据库操作）
            Order order = Order.builder()
                    .orderId(messageDTO.getOrderId())
                    .amount(messageDTO.getAmount())
                    .userId(messageDTO.getUserId())
                    .content(messageDTO.getContent())
                    .build();

            boolean success = orderService.createOrderWithTransaction(order);

            if (success) {
                log.info("本地事务成功，提交消息 - OrderId: {}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            } else {
                log.error("本地事务失败，回滚消息 - OrderId: {}", orderId);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

        } catch (Exception e) {
            log.error("本地事务异常，回滚消息 - OrderId: {}", orderId, e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    /**
     * 事务回查（Broker未收到最终状态时调用）
     *
     * 当以下情况发生时，Broker会主动回查：
     * 1. executeLocalTransaction 返回 UNKNOWN
     * 2. executeLocalTransaction 执行超时
     *
     * @param msg 消息
     * @return 事务状态：COMMIT（提交）/ ROLLBACK（回滚）
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        String messageJson = new String((byte[]) msg.getPayload());
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);
        String orderId = messageDTO.getOrderId();

        log.info("事务回查 - OrderId: {}", orderId);

        // 检查订单是否存在
        boolean exists = orderService.checkOrderExists(orderId);

        if (exists) {
            log.info("事务回查成功，提交消息 - OrderId: {}", orderId);
            return RocketMQLocalTransactionState.COMMIT;
        } else {
            log.info("事务回查失败，回滚消息 - OrderId: {}", orderId);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }
}