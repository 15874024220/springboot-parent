package com.example.kafka.service;

import com.example.kafka.model.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 订单服务
 * 模拟订单的业务处理逻辑
 */
@Slf4j
@Service
public class OrderService {

    // 模拟订单存储
    private final Map<String, OrderMessage> orderStore = new ConcurrentHashMap<>();

    /**
     * 创建订单
     */
    public void createOrder(OrderMessage order) {
        log.info("创建订单: orderId={}, userId={}, product={}, amount={}",
                order.getOrderId(), order.getUserId(), order.getProductName(), order.getAmount());

        order.setStatus(0); // 创建状态
        orderStore.put(order.getOrderId(), order);
    }

    /**
     * 更新订单状态
     */
    public void updateOrderStatus(String orderId, Integer status) {
        OrderMessage order = orderStore.get(orderId);
        if (order != null) {
            order.setStatus(status);
            log.info("更新订单状态: orderId={}, status={}", orderId, status);
        } else {
            log.warn("订单不存在: orderId={}", orderId);
            throw new RuntimeException("订单不存在");
        }
    }

    /**
     * 查询订单
     */
    public OrderMessage getOrder(String orderId) {
        return orderStore.get(orderId);
    }

    /**
     * 处理订单支付
     */
    public void processPayment(String orderId, Long amount) {
        OrderMessage order = orderStore.get(orderId);
        if (order == null) {
            throw new RuntimeException("订单不存在");
        }
        if (order.getStatus() != 0) {
            throw new RuntimeException("订单状态不正确，无法支付");
        }
        if (!order.getAmount().equals(amount)) {
            throw new RuntimeException("支付金额不正确");
        }

        order.setStatus(1); // 已支付
        log.info("订单支付成功: orderId={}, amount={}", orderId, amount);
    }
}