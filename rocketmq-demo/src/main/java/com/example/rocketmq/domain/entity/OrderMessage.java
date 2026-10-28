package com.example.rocketmq.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    // 业务唯一键，用于幂等去重（如订单号）
    private String orderId;
    // 消息ID（全局唯一，由业务生成或RocketMQ生成）
    private String messageId;
    // 业务状态（1-创建，2-支付，3-完成）
    private Integer status;
    // 消息内容
    private String content;
    // 时间戳
    private Long timestamp;
}