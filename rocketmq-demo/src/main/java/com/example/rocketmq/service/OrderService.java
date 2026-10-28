package com.example.rocketmq.service;

import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.domain.entity.Order;

/**
 * 订单服务接口
 *
 * 定义订单相关的业务逻辑
 *
 * @author example
 * 
 */
public interface OrderService {

    /**
     * 处理普通订单消息
     *
     * @param messageDTO 消息内容
     * @return 处理结果
     */
    boolean processOrderMessage(OrderMessageDTO messageDTO);

    /**
     * 顺序处理订单状态流转（用于顺序消息）
     *
     * @param messageDTO 消息内容
     * @return 处理结果
     */
    boolean processOrderStatusSequentially(OrderMessageDTO messageDTO);

    /**
     * 创建订单（事务消息用）
     *
     * @param order 订单实体
     * @return 是否成功
     */
    boolean createOrderWithTransaction(Order order);

    /**
     * 检查订单是否存在（事务回查用）
     *
     * @param orderId 订单号
     * @return 是否存在
     */
    boolean checkOrderExists(String orderId);

    /**
     * 更新订单状态
     *
     * @param orderId 订单号
     * @param status 新状态
     * @return 是否成功
     */
    boolean updateOrderStatus(String orderId, Integer status);

    /**
     * 获取订单详情
     *
     * @param orderId 订单号
     * @return 订单实体
     */
    Order getOrderById(String orderId);
}