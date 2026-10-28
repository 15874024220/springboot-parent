package com.example.rocketmq.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.domain.entity.Order;
import com.example.rocketmq.domain.enums.OrderStatusEnum;
import com.example.rocketmq.exception.BusinessException;
import com.example.rocketmq.mapper.OrderMapper;
import com.example.rocketmq.service.OrderService;
import com.example.rocketmq.util.TraceIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 订单服务实现类
 *
 * 核心业务逻辑实现：
 * 1. 订单创建
 * 2. 订单支付
 * 3. 订单完成
 * 4. 订单取消
 * 5. 状态流转校验
 *
 * @author example
 * 
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order>
        implements OrderService {

    private final OrderMapper orderMapper;

    /**
     * 处理普通订单消息
     *
     * 业务流程：
     * 1. 根据订单状态分发到不同的处理器
     * 2. 创建订单：检查是否已存在
     * 3. 支付订单：更新订单状态为已支付
     * 4. 完成订单：更新订单状态为已完成
     * 5. 取消订单：更新订单状态为已取消
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean processOrderMessage(OrderMessageDTO messageDTO) {
        String orderId = messageDTO.getOrderId();
        String traceId = TraceIdUtil.getOrCreateTraceId();

        log.info("处理订单消息 - TraceId: {}, OrderId: {}, Status: {}",
                traceId, orderId, messageDTO.getStatus());

        try {
            // 查询现有订单
            Order existingOrder = this.getOrderById(orderId);
            Integer status = messageDTO.getStatus();

            if (status == null) {
                log.error("消息状态为空 - OrderId: {}", orderId);
                return false;
            }

            // 根据状态分发处理
            OrderStatusEnum statusEnum = OrderStatusEnum.fromCode(status);

            switch (statusEnum) {
                case CREATED:
                    // 创建订单：如果订单已存在则跳过
                    if (existingOrder != null) {
                        log.warn("订单已存在，跳过创建 - OrderId: {}", orderId);
                        return true;
                    }
                    return createOrder(messageDTO);

                case PAID:
                    // 支付订单：订单必须存在
                    if (existingOrder == null) {
                        log.error("订单不存在，无法支付 - OrderId: {}", orderId);
                        return false;
                    }
                    return processPayment(orderId);

                case COMPLETED:
                    // 完成订单：订单必须存在
                    if (existingOrder == null) {
                        log.error("订单不存在，无法完成 - OrderId: {}", orderId);
                        return false;
                    }
                    return completeOrder(orderId);

                case CANCELLED:
                    // 取消订单：订单不存在也返回成功（幂等）
                    if (existingOrder == null) {
                        log.warn("订单不存在，取消操作忽略 - OrderId: {}", orderId);
                        return true;
                    }
                    return cancelOrder(orderId);

                default:
                    log.error("未知状态 - OrderId: {}, Status: {}", orderId, status);
                    return false;
            }
        } catch (Exception e) {
            log.error("处理订单消息异常 - TraceId: {}, OrderId: {}", traceId, orderId, e);
            throw new BusinessException("订单处理失败: " + e.getMessage(), e);
        }
    }

    /**
     * 顺序处理订单状态流转（用于顺序消息）
     *
     * 特点：
     * 1. 保证订单状态的顺序性：创建 -> 支付 -> 完成
     * 2. 状态流转校验
     * 3. 支持幂等处理
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean processOrderStatusSequentially(OrderMessageDTO messageDTO) {
        String orderId = messageDTO.getOrderId();
        Integer newStatus = messageDTO.getStatus();

        log.info("顺序处理订单状态 - OrderId: {}, NewStatus: {}", orderId, newStatus);

        // 查询当前订单状态
        Order order = this.getOrderById(orderId);

        // 订单不存在且是创建消息，则创建订单
        if (order == null && newStatus == OrderStatusEnum.CREATED.getCode()) {
            return createOrder(messageDTO);
        }

        // 订单不存在且不是创建消息，返回失败
        if (order == null) {
            log.error("订单不存在 - OrderId: {}", orderId);
            return false;
        }

        Integer currentStatus = order.getStatus();

        // 状态流转校验
        if (!OrderStatusEnum.isValidTransition(currentStatus, newStatus)) {
            log.error("非法状态流转 - OrderId: {}, Current: {}, New: {}",
                    orderId, currentStatus, newStatus);
            return false;
        }

        // 根据新状态分发处理
        OrderStatusEnum statusEnum = OrderStatusEnum.fromCode(newStatus);

        switch (statusEnum) {
            case PAID:
                return processPayment(orderId);
            case COMPLETED:
                return completeOrder(orderId);
            case CANCELLED:
                return cancelOrder(orderId);
            default:
                return true;
        }
    }

    /**
     * 创建订单（用于事务消息）
     *
     * 特点：
     * 1. 支持幂等：如果订单已存在则返回成功
     * 2. 事务保证：与消息发送在同一事务中
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean createOrderWithTransaction(Order order) {
        log.info("事务消息创建订单 - OrderId: {}", order.getOrderId());

        // 检查订单是否已存在（幂等）
        Order existing = this.getOrderById(order.getOrderId());
        if (existing != null) {
            log.info("订单已存在，无需重复创建 - OrderId: {}", order.getOrderId());
            return true;
        }

        // 设置订单初始状态
        order.setStatus(OrderStatusEnum.CREATED.getCode());
        order.setCreateTime(LocalDateTime.now());
        order.setUpdateTime(LocalDateTime.now());

        // 保存订单
        return this.save(order);
    }

    /**
     * 检查订单是否存在（事务回查使用）
     */
    @Override
    public boolean checkOrderExists(String orderId) {
        Order order = this.getOrderById(orderId);
        log.info("事务回查订单状态 - OrderId: {}, Exists: {}", orderId, order != null);
        return order != null;
    }

    /**
     * 更新订单状态
     * 使用MyBatis-Plus的乐观锁机制
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean updateOrderStatus(String orderId, Integer status) {
        // 先查询订单获取版本号
        Order order = this.getOrderById(orderId);
        if (order == null) {
            return false;
        }

        // 使用乐观锁更新
        LambdaUpdateWrapper<Order> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(Order::getOrderId, orderId)
                .eq(Order::getVersion, order.getVersion())  // 乐观锁条件
                .set(Order::getStatus, status)
                .set(Order::getUpdateTime, LocalDateTime.now());

        return this.update(wrapper);
    }

    /**
     * 获取订单详情
     */
    @Override
    public Order getOrderById(String orderId) {
        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(Order::getOrderId, orderId)
                .eq(Order::getDeleted, 0);
        return this.getOne(wrapper);
    }

    /**
     * 创建订单
     */
    private boolean createOrder(OrderMessageDTO messageDTO) {
        Order order = Order.builder()
                .orderId(messageDTO.getOrderId())
                .status(OrderStatusEnum.CREATED.getCode())
                .amount(messageDTO.getAmount())
                .userId(messageDTO.getUserId())
                .content(messageDTO.getContent())
                .build();

        return this.save(order);
    }

    /**
     * 处理支付
     */
    private boolean processPayment(String orderId) {
        return updateOrderStatus(orderId, OrderStatusEnum.PAID.getCode());
    }

    /**
     * 完成订单
     */
    private boolean completeOrder(String orderId) {
        return updateOrderStatus(orderId, OrderStatusEnum.COMPLETED.getCode());
    }

    /**
     * 取消订单
     */
    private boolean cancelOrder(String orderId) {
        return updateOrderStatus(orderId, OrderStatusEnum.CANCELLED.getCode());
    }
}