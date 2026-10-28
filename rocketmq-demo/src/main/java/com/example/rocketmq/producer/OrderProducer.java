package com.example.rocketmq.producer;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.constant.MQConstant;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 订单消息生产者
 *
 * 提供多种发送方式：
 * 1. 同步发送：等待Broker响应，适合重要业务
 * 2. 异步发送：不等待响应，适合高并发场景
 * 3. 顺序发送：保证相同Key的消息进入同一队列
 * 4. 事务消息：分布式事务解决方案
 * 5. 批量发送：提升吞吐量
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final RocketMQTemplate rocketMQTemplate;

    @Value("${order.topic}")
    private String topic;

    @Value("${order.normal-tag}")
    private String normalTag;

    @Value("${order.orderly-tag}")
    private String orderlyTag;

    @Value("${order.transaction-tag}")
    private String transactionTag;

    /**
     * 1. 同步发送消息
     *
     * 特点：
     * - 阻塞等待Broker响应
     * - 可以获取发送结果（MsgId、QueueId等）
     * - 发送失败会抛出异常
     *
     * 适用场景：
     * - 重要业务通知
     * - 订单状态变更
     * - 需要确保消息发送成功的场景
     *
     * @param messageDTO 消息内容
     * @return SendResult 发送结果
     * @throws RuntimeException 发送失败时抛出
     */
    public SendResult syncSend(OrderMessageDTO messageDTO) {
        // 构建目标：topic:tag 格式
        String destination = topic + ":" + normalTag;

        // 生成唯一消息ID（用于幂等性）
        if (messageDTO.getMessageId() == null) {
            messageDTO.setMessageId(IdUtil.fastSimpleUUID());
        }
        messageDTO.setTimestamp(System.currentTimeMillis());

        // 构建RocketMQ消息
        // - payload：消息体（JSON字符串）
        // - KEYS：业务Key，用于消息查询和顺序消费
        // - messageId：自定义属性，用于幂等性
        Message<String> message = MessageBuilder
                .withPayload(JSON.toJSONString(messageDTO))
                .setHeader(MQConstant.KEYS_HEADER, messageDTO.getOrderId())
                .setHeader(MQConstant.MESSAGE_ID_HEADER, messageDTO.getMessageId())
                .build();

        log.info("开始同步发送消息 - OrderId: {}, MessageId: {}",
                messageDTO.getOrderId(), messageDTO.getMessageId());

        // 同步发送消息
        SendResult sendResult = rocketMQTemplate.syncSend(destination, message);

        // 检查发送结果
        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
            log.info("同步发送成功 - MsgId: {}, OrderId: {}, QueueId: {}",
                    sendResult.getMsgId(),
                    messageDTO.getOrderId(),
                    sendResult.getMessageQueue().getQueueId());
            return sendResult;
        } else {
            log.error("同步发送失败 - Status: {}, MsgId: {}, OrderId: {}",
                    sendResult.getSendStatus(),
                    sendResult.getMsgId(),
                    messageDTO.getOrderId());
            throw new RuntimeException("消息发送失败: " + sendResult.getSendStatus());
        }
    }

    /**
     * 2. 异步发送消息
     *
     * 特点：
     * - 非阻塞，立即返回
     * - 通过回调获取发送结果
     * - 性能更高
     *
     * 适用场景：
     * - 日志收集
     * - 埋点数据
     * - 非关键通知
     *
     * @param messageDTO 消息内容
     * @param callback 发送回调（可空）
     */
    public void asyncSend(OrderMessageDTO messageDTO, SendCallback callback) {
        String destination = topic + ":" + normalTag;

        if (messageDTO.getMessageId() == null) {
            messageDTO.setMessageId(IdUtil.fastSimpleUUID());
        }
        messageDTO.setTimestamp(System.currentTimeMillis());

        Message<String> message = MessageBuilder
                .withPayload(JSON.toJSONString(messageDTO))
                .setHeader(MQConstant.KEYS_HEADER, messageDTO.getOrderId())
                .setHeader(MQConstant.MESSAGE_ID_HEADER, messageDTO.getMessageId())
                .build();

        log.info("开始异步发送消息 - OrderId: {}", messageDTO.getOrderId());

        // 异步发送
        rocketMQTemplate.asyncSend(destination, message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("异步发送成功 - MsgId: {}, OrderId: {}",
                        sendResult.getMsgId(), messageDTO.getOrderId());
                if (callback != null) {
                    callback.onSuccess(sendResult);
                }
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("异步发送失败 - OrderId: {}", messageDTO.getOrderId(), throwable);
                if (callback != null) {
                    callback.onException(throwable);
                }
            }
        });
    }

    /**
     * 3. 顺序发送消息
     *
     * 特点：
     * - 保证相同HashKey的消息进入同一个MessageQueue
     * - 同一个队列的消息被顺序消费
     *
     * 适用场景：
     * - 订单状态流转（创建 -> 支付 -> 完成）
     * - 数据库binlog同步
     * - 需要保证顺序的业务
     *
     * @param messageDTO 消息内容
     * @param hashKey 用于选择队列的HashKey（如订单号、用户ID）
     * @return SendResult 发送结果
     */
    public SendResult syncSendOrderly(OrderMessageDTO messageDTO, String hashKey) {
        String destination = topic + ":" + orderlyTag;

        if (messageDTO.getMessageId() == null) {
            messageDTO.setMessageId(IdUtil.fastSimpleUUID());
        }
        messageDTO.setTimestamp(System.currentTimeMillis());

        Message<String> message = MessageBuilder
                .withPayload(JSON.toJSONString(messageDTO))
                .setHeader(MQConstant.KEYS_HEADER, messageDTO.getOrderId())
                .setHeader(MQConstant.MESSAGE_ID_HEADER, messageDTO.getMessageId())
                .build();

        log.info("开始发送顺序消息 - OrderId: {}, HashKey: {}",
                messageDTO.getOrderId(), hashKey);

        // 顺序发送：相同的hashKey会发送到同一个MessageQueue
        SendResult sendResult = rocketMQTemplate.syncSendOrderly(destination, message, hashKey);

        log.info("顺序消息发送完成 - OrderId: {}, HashKey: {}, QueueId: {}",
                messageDTO.getOrderId(),
                hashKey,
                sendResult.getMessageQueue().getQueueId());

        return sendResult;
    }

    /**
     * 4. 发送事务消息
     *
     * 特点：
     * - 消息发送与本地事务原子性
     * - 支持事务回查
     *
     * 适用场景：
     * - 分布式事务最终一致性
     * - 跨服务的数据一致性
     *
     * @param messageDTO 消息内容
     */
    public void sendTransactionMsg(OrderMessageDTO messageDTO) {
        String destination = topic + ":" + transactionTag;

        if (messageDTO.getMessageId() == null) {
            messageDTO.setMessageId(IdUtil.fastSimpleUUID());
        }
        messageDTO.setTimestamp(System.currentTimeMillis());

        Message<String> message = MessageBuilder
                .withPayload(JSON.toJSONString(messageDTO))
                .setHeader(MQConstant.KEYS_HEADER, messageDTO.getOrderId())
                .setHeader(MQConstant.MESSAGE_ID_HEADER, messageDTO.getMessageId())
                .build();

        log.info("发送事务消息 - OrderId: {}, MessageId: {}",
                messageDTO.getOrderId(), messageDTO.getMessageId());

        // 发送事务消息，第三个参数为透传的业务参数
        rocketMQTemplate.sendMessageInTransaction(destination, message, messageDTO.getOrderId());
    }

    /**
     * 5. 批量发送消息
     *
     * 特点：
     * - 一次发送多条消息
     * - 提升吞吐量
     * - 批量消息需满足：同一Topic、同一Tag、不能是事务消息
     *
     * 适用场景：
     * - 大量小消息发送
     * - 数据同步
     *
     * @param messageList 消息列表
     */
    public void batchSend(List<OrderMessageDTO> messageList) {
        if (messageList == null || messageList.isEmpty()) {
            log.warn("批量发送消息列表为空");
            return;
        }

        String destination = topic + ":" + normalTag;
        List<Message<String>> messages = messageList.stream()
                .map(dto -> {
                    if (dto.getMessageId() == null) {
                        dto.setMessageId(IdUtil.fastSimpleUUID());
                    }
                    dto.setTimestamp(System.currentTimeMillis());
                    return MessageBuilder
                            .withPayload(JSON.toJSONString(dto))
                            .setHeader(MQConstant.KEYS_HEADER, dto.getOrderId())
                            .setHeader(MQConstant.MESSAGE_ID_HEADER, dto.getMessageId())
                            .build();
                })
                .toList();

        log.info("批量发送消息 - 数量: {}", messageList.size());

        // 批量发送
        rocketMQTemplate.syncSend(destination, messages);
        log.info("批量发送完成");
    }
}