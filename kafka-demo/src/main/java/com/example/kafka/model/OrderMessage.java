package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户订单消息示例
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage {
    private String orderId;
    private Long userId;
    private String productName;
    private Integer quantity;
    private Long amount;
    private Integer status; // 0:创建 1:支付 2:完成 3:取消
}
