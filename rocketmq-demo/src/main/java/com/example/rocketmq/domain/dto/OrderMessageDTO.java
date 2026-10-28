package com.example.rocketmq.domain.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/**
 * 订单消息DTO
 *
 * 用于RocketMQ消息传输
 * 特点：
 * 1. 实现Serializable接口，支持序列化
 * 2. 包含消息ID用于幂等性
 * 3. 包含业务Key用于顺序消费
 * 4. 使用Jakarta Validation注解进行参数校验
 *
 * @author example
 * 
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessageDTO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 消息ID（全局唯一）
     * 用于幂等性校验，防止消息重复消费
     * 由生产者生成，使用UUID
     */
    private String messageId;

    /**
     * 订单号（业务Key）
     * 用于顺序消费的HashKey和业务查询
     * 必须唯一
     */
    @NotBlank(message = "订单号不能为空")
    private String orderId;

    /**
     * 订单状态
     * 1-创建 2-支付 3-完成 4-取消
     */
    @NotNull(message = "订单状态不能为空")
    private Integer status;

    /** 订单金额（分） */
    private Long amount;

    /** 用户ID */
    private String userId;

    /** 消息内容（JSON格式） */
    private String content;

    /** 消息时间戳 */
    private Long timestamp;

    /** 重试次数（消费端使用） */
    private Integer retryTimes;
}