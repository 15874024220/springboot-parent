package com.example.rocketmq.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.rocketmq.constant.MQConstant;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 死信队列消费者
 *
 * 死信队列说明：
 * 1. 当消息重试次数超过maxReconsumeTimes后，会进入死信队列
 * 2. 死信队列Topic格式：%DLQ%{原消费者组名}
 * 3. 死信消息需要人工介入处理
 *
 * 处理策略：
 * 1. 记录到数据库，标记为需要人工处理
 * 2. 发送告警通知（钉钉、邮件、短信）
 * 3. 存储到Elasticsearch便于后续分析
 *
 * @author example
 * 
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = MQConstant.DLQ_PREFIX + MQConstant.NORMAL_CONSUMER_GROUP,
        consumerGroup = MQConstant.DLQ_CONSUMER_GROUP,
        maxReconsumeTimes = 1  // 死信队列只消费一次，失败则丢弃
)
public class DeadLetterConsumer implements RocketMQListener<String> {

    /**
     * 处理死信消息
     *
     * @param messageJson 死信消息内容
     */
    @Override
    public void onMessage(String messageJson) {
        OrderMessageDTO messageDTO = JSON.parseObject(messageJson, OrderMessageDTO.class);

        log.error("收到死信消息 - OrderId: {}, MessageId: {}, Content: {}",
                messageDTO.getOrderId(),
                messageDTO.getMessageId(),
                messageDTO.getContent());

        // 1. 保存到死信表（等待人工处理）
        saveToDeadLetterTable(messageDTO);

        // 2. 发送告警通知
        sendAlert(messageDTO);

        // 3. 可选：记录到ES便于分析
        // saveToElasticsearch(messageDTO);
    }

    /**
     * 保存死信消息到数据库
     */
    private void saveToDeadLetterTable(OrderMessageDTO messageDTO) {
        // TODO: 保存到专门的死信表
        log.info("保存死信消息到数据库 - OrderId: {}", messageDTO.getOrderId());
        // INSERT INTO dead_letter_message (message_id, order_id, content, create_time) VALUES (...)
    }

    /**
     * 发送告警通知
     */
    private void sendAlert(OrderMessageDTO messageDTO) {
        // TODO: 发送告警（钉钉机器人、邮件、短信）
        log.warn("发送死信告警 - OrderId: {}, MessageId: {}",
                messageDTO.getOrderId(), messageDTO.getMessageId());

        // 示例：钉钉告警
        // String alertMsg = String.format("【RocketMQ告警】死信消息：订单号=%s，消息ID=%s",
        //         messageDTO.getOrderId(), messageDTO.getMessageId());
        // dingTalkService.sendAlert(alertMsg);
    }
}