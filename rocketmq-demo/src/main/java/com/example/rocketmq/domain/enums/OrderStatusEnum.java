package com.example.rocketmq.domain.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 订单状态枚举
 *
 * 定义订单的所有状态及其流转规则
 *
 * @author example
 * 
 */
@Getter
public enum OrderStatusEnum {

    CREATED(1, "已创建", new Integer[]{2, 4}),      // 创建后可支付或取消
    PAID(2, "已支付", new Integer[]{3}),            // 支付后可完成
    COMPLETED(3, "已完成", new Integer[]{}),        // 完成终态
    CANCELLED(4, "已取消", new Integer[]{});        // 取消终态

    /** 状态码 */
    private final Integer code;

    /** 状态描述 */
    private final String desc;

    /** 允许的下一个状态 */
    private final Integer[] nextStatuses;

    /** 状态码到枚举的映射缓存 */
    private static final Map<Integer, OrderStatusEnum> CODE_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(OrderStatusEnum::getCode, Function.identity()));

    OrderStatusEnum(Integer code, String desc, Integer[] nextStatuses) {
        this.code = code;
        this.desc = desc;
        this.nextStatuses = nextStatuses;
    }

    /**
     * 根据状态码获取枚举
     * @param code 状态码
     * @return 枚举值
     * @throws IllegalArgumentException 如果状态码不存在
     */
    public static OrderStatusEnum fromCode(Integer code) {
        OrderStatusEnum status = CODE_MAP.get(code);
        if (status == null) {
            throw new IllegalArgumentException("未知的订单状态码: " + code);
        }
        return status;
    }

    /**
     * 检查状态流转是否合法
     * @param currentStatus 当前状态
     * @param newStatus 新状态
     * @return 是否合法
     */
    public static boolean isValidTransition(Integer currentStatus, Integer newStatus) {
        OrderStatusEnum current = fromCode(currentStatus);
        Integer[] nextStatuses = current.getNextStatuses();
        return Arrays.asList(nextStatuses).contains(newStatus);
    }
}