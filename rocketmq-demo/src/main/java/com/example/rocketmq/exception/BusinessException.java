package com.example.rocketmq.exception;

import lombok.Getter;

/**
 * 业务异常类
 *
 * 功能说明：
 * 1. 封装业务层抛出的异常
 * 2. 支持错误码和错误信息
 * 3. 便于前端根据错误码进行不同处理
 *
 * 使用场景：
 * - 订单不存在
 * - 状态流转非法
 * - 库存不足
 * - 参数校验失败
 *
 * @author example
 * @date 2024
 */
@Getter
public class BusinessException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     * 200: 成功
     * 400: 参数错误
     * 401: 未授权
     * 403: 禁止访问
     * 404: 资源不存在
     * 500: 系统错误
     * 1001-1999: 订单相关错误
     * 2001-2999: 库存相关错误
     * 3001-3999: 消息相关错误
     */
    private final Integer code;

    /**
     * 错误信息
     */
    private final String message;

    /**
     * 原始异常
     */
    private final Throwable cause;

    /**
     * 构造器 - 只有错误信息（默认错误码500）
     *
     * @param message 错误信息
     */
    public BusinessException(String message) {
        super(message);
        this.code = 500;
        this.message = message;
        this.cause = null;
    }

    /**
     * 构造器 - 错误码和错误信息
     *
     * @param code    错误码
     * @param message 错误信息
     */
    public BusinessException(Integer code, String message) {
        super(message);
        this.code = code;
        this.message = message;
        this.cause = null;
    }

    /**
     * 构造器 - 错误信息和原始异常
     *
     * @param message 错误信息
     * @param cause   原始异常
     */
    public BusinessException(String message, Throwable cause) {
        super(message, cause);
        this.code = 500;
        this.message = message;
        this.cause = cause;
    }

    /**
     * 构造器 - 错误码、错误信息和原始异常
     *
     * @param code    错误码
     * @param message 错误信息
     * @param cause   原始异常
     */
    public BusinessException(Integer code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
        this.cause = cause;
    }

    /**
     * 构造器 - 错误码枚举
     *
     * @param errorCode 错误码枚举
     */
    public BusinessException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.code = errorCode.getCode();
        this.message = errorCode.getMessage();
        this.cause = null;
    }

    /**
     * 构造器 - 错误码枚举和原始异常
     *
     * @param errorCode 错误码枚举
     * @param cause     原始异常
     */
    public BusinessException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.code = errorCode.getCode();
        this.message = errorCode.getMessage();
        this.cause = cause;
    }

    /**
     * 构造器 - 错误码枚举和动态参数
     *
     * @param errorCode 错误码枚举
     * @param args      动态参数（用于格式化错误信息）
     */
    public BusinessException(ErrorCode errorCode, Object... args) {
        super(String.format(errorCode.getMessage(), args));
        this.code = errorCode.getCode();
        this.message = String.format(errorCode.getMessage(), args);
        this.cause = null;
    }

    /**
     * 错误码枚举
     * 集中管理所有业务错误码
     */
    @Getter
    public enum ErrorCode {

        // ==================== 系统级错误 (1xxx) ====================
        SUCCESS(200, "成功"),
        PARAM_ERROR(400, "参数错误: %s"),
        UNAUTHORIZED(401, "未授权访问"),
        FORBIDDEN(403, "禁止访问"),
        NOT_FOUND(404, "资源不存在: %s"),
        SYSTEM_ERROR(500, "系统繁忙，请稍后重试"),

        // ==================== 订单相关错误 (1001-1999) ====================
        ORDER_NOT_FOUND(1001, "订单不存在: %s"),
        ORDER_STATUS_INVALID(1002, "订单状态非法: %s"),
        ORDER_STATUS_TRANSITION_INVALID(1003, "订单状态流转非法: 当前状态[%s] 不能转换为[%s]"),
        ORDER_ALREADY_EXISTS(1004, "订单已存在: %s"),
        ORDER_CREATE_FAILED(1005, "订单创建失败: %s"),
        ORDER_UPDATE_FAILED(1006, "订单更新失败: %s"),
        ORDER_CANCEL_FAILED(1007, "订单取消失败: %s"),
        ORDER_PAY_FAILED(1008, "订单支付失败: %s"),
        ORDER_COMPLETE_FAILED(1009, "订单完成失败: %s"),
        ORDER_AMOUNT_INVALID(1010, "订单金额非法: %s"),

        // ==================== 库存相关错误 (2001-2999) ====================
        STOCK_INSUFFICIENT(2001, "库存不足: 商品[%s], 可用库存[%d], 需要[%d]"),
        STOCK_DEDUCT_FAILED(2002, "库存扣减失败: %s"),
        STOCK_ROLLBACK_FAILED(2003, "库存回滚失败: %s"),

        // ==================== 消息相关错误 (3001-3999) ====================
        MESSAGE_SEND_FAILED(3001, "消息发送失败: %s"),
        MESSAGE_CONSUME_FAILED(3002, "消息消费失败: %s"),
        MESSAGE_DUPLICATE(3003, "消息重复消费: %s"),
        MESSAGE_PARSE_FAILED(3004, "消息解析失败: %s"),
        TRANSACTION_EXECUTE_FAILED(3005, "事务消息执行失败: %s"),
        TRANSACTION_CHECK_FAILED(3006, "事务消息回查失败: %s"),

        // ==================== 用户相关错误 (4001-4999) ====================
        USER_NOT_FOUND(4001, "用户不存在: %s"),
        USER_DISABLED(4002, "用户已被禁用: %s"),

        // ==================== 支付相关错误 (5001-5999) ====================
        PAYMENT_FAILED(5001, "支付失败: %s"),
        PAYMENT_TIMEOUT(5002, "支付超时: %s"),

        // ==================== 缓存相关错误 (6001-6999) ====================
        CACHE_GET_FAILED(6001, "缓存获取失败: %s"),
        CACHE_SET_FAILED(6002, "缓存设置失败: %s"),
        CACHE_DELETE_FAILED(6003, "缓存删除失败: %s"),
        LOCK_ACQUIRE_FAILED(6004, "获取分布式锁失败: %s"),

        // ==================== 数据库相关错误 (7001-7999) ====================
        DB_OPERATION_FAILED(7001, "数据库操作失败: %s"),
        DB_DUPLICATE_KEY(7002, "数据重复: %s");

        private final Integer code;
        private final String message;

        ErrorCode(Integer code, String message) {
            this.code = code;
            this.message = message;
        }
    }
}