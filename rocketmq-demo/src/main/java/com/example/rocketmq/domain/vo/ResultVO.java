package com.example.rocketmq.domain.vo;

import lombok.Data;

/**
 * 统一响应对象
 *
 * @author example
 * 
 */
@Data
public class ResultVO<T> {

    /** 状态码：200-成功，其他-失败 */
    private Integer code;

    /** 响应消息 */
    private String message;

    /** 响应数据 */
    private T data;

    /** 时间戳 */
    private Long timestamp;

    private ResultVO() {
        this.timestamp = System.currentTimeMillis();
    }

    private ResultVO(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 成功响应（无数据）
     */
    public static <T> ResultVO<T> success() {
        return new ResultVO<>(200, "success", null);
    }

    /**
     * 成功响应（有数据）
     */
    public static <T> ResultVO<T> success(T data) {
        return new ResultVO<>(200, "success", data);
    }

    /**
     * 成功响应（自定义消息）
     */
    public static <T> ResultVO<T> success(String message) {
        return new ResultVO<>(200, message, null);
    }

    /**
     * 失败响应
     */
    public static <T> ResultVO<T> error(String message) {
        return new ResultVO<>(500, message, null);
    }

    /**
     * 失败响应（自定义状态码）
     */
    public static <T> ResultVO<T> error(Integer code, String message) {
        return new ResultVO<>(code, message, null);
    }
}