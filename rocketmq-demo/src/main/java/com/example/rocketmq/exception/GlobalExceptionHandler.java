package com.example.rocketmq.exception;

import com.example.rocketmq.domain.vo.ResultVO;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.stream.Collectors;

/**
 * 全局异常处理器
 *
 * 功能说明：
 * 1. 统一处理Controller层抛出的异常
 * 2. 将异常转换为统一的ResultVO格式返回
 * 3. 记录异常日志，便于问题排查
 *
 * 处理的异常类型：
 * - 业务异常（BusinessException）
 * - 参数校验异常（Valid异常）
 * - 数据库异常
 * - RocketMQ异常
 * - 系统异常
 *
 * @author example
 * @date 2024
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 业务异常处理
     *
     * @param e 业务异常
     * @return 统一响应结果
     */
    @ExceptionHandler(BusinessException.class)
    public ResultVO<Void> handleBusinessException(BusinessException e) {
        log.warn("业务异常 - Code: {}, Message: {}", e.getCode(), e.getMessage());
        return ResultVO.error(e.getCode(), e.getMessage());
    }

    /**
     * 参数校验异常处理（@Valid 注解校验失败）
     *
     * 适用场景：使用 @Valid 注解的请求体参数校验失败
     *
     * @param e 参数校验异常
     * @return 统一响应结果
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResultVO<Void> handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        String errorMsg = e.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        log.warn("参数校验失败 - {}", errorMsg);
        return ResultVO.error(400, "参数校验失败: " + errorMsg);
    }

    /**
     * 参数绑定异常处理（@RequestParam 参数绑定失败）
     *
     * @param e 参数绑定异常
     * @return 统一响应结果
     */
    @ExceptionHandler(BindException.class)
    public ResultVO<Void> handleBindException(BindException e) {
        String errorMsg = e.getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        log.warn("参数绑定失败 - {}", errorMsg);
        return ResultVO.error(400, "参数绑定失败: " + errorMsg);
    }

    /**
     * 参数缺失异常处理
     *
     * @param e 参数缺失异常
     * @return 统一响应结果
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResultVO<Void> handleMissingServletRequestParameterException(
            MissingServletRequestParameterException e) {
        log.warn("参数缺失 - ParameterName: {}, ParameterType: {}",
                e.getParameterName(), e.getParameterType());
        return ResultVO.error(400, "缺少必要参数: " + e.getParameterName());
    }

    /**
     * 参数类型不匹配异常处理
     *
     * @param e 参数类型不匹配异常
     * @return 统一响应结果
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResultVO<Void> handleMethodArgumentTypeMismatchException(
            MethodArgumentTypeMismatchException e) {
        log.warn("参数类型不匹配 - Name: {}, Value: {}, RequiredType: {}",
                e.getName(), e.getValue(), e.getRequiredType());
        return ResultVO.error(400, String.format("参数[%s]类型错误，期望类型: %s",
                e.getName(), e.getRequiredType().getSimpleName()));
    }

    /**
     * 请求体解析异常处理
     *
     * @param e 请求体解析异常
     * @return 统一响应结果
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResultVO<Void> handleHttpMessageNotReadableException(HttpMessageNotReadableException e) {
        log.warn("请求体解析失败", e);
        return ResultVO.error(400, "请求体格式错误: " + e.getMessage());
    }

    /**
     * 约束违反异常处理（用于方法参数上的 @Validated 注解）
     *
     * @param e 约束违反异常
     * @return 统一响应结果
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResultVO<Void> handleConstraintViolationException(ConstraintViolationException e) {
        String errorMsg = e.getConstraintViolations().stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining("; "));
        log.warn("约束校验失败 - {}", errorMsg);
        return ResultVO.error(400, "参数校验失败: " + errorMsg);
    }

    /**
     * 数据库唯一键冲突异常处理
     *
     * @param e 唯一键冲突异常
     * @return 统一响应结果
     */
    @ExceptionHandler(DuplicateKeyException.class)
    public ResultVO<Void> handleDuplicateKeyException(DuplicateKeyException e) {
        log.warn("数据重复", e);
        return ResultVO.error(409, "数据已存在，请勿重复提交");
    }

    /**
     * SQL约束违反异常处理
     *
     * @param e SQL约束违反异常
     * @return 统一响应结果
     */
    @ExceptionHandler(SQLIntegrityConstraintViolationException.class)
    public ResultVO<Void> handleSQLIntegrityConstraintViolationException(
            SQLIntegrityConstraintViolationException e) {
        log.warn("SQL约束违反", e);
        return ResultVO.error(409, "数据操作失败: " + e.getMessage());
    }

    /**
     * RocketMQ客户端异常处理
     *
     * @param e RocketMQ异常
     * @return 统一响应结果
     */
    @ExceptionHandler(MQClientException.class)
    public ResultVO<Void> handleMQClientException(MQClientException e) {
        log.error("RocketMQ客户端异常 - ResponseCode: {}, ErrorMessage: {}",
                e.getResponseCode(), e.getErrorMessage(), e);
        return ResultVO.error(500, "消息服务异常，请稍后重试");
    }

    /**
     * IllegalStateException处理
     *
     * @param e 非法状态异常
     * @return 统一响应结果
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResultVO<Void> handleIllegalStateException(IllegalStateException e) {
        log.warn("非法状态异常", e);
        return ResultVO.error(400, "操作失败: " + e.getMessage());
    }

    /**
     * IllegalArgumentException处理
     *
     * @param e 非法参数异常
     * @return 统一响应结果
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResultVO<Void> handleIllegalArgumentException(IllegalArgumentException e) {
        log.warn("非法参数异常", e);
        return ResultVO.error(400, "参数错误: " + e.getMessage());
    }

    /**
     * NullPointerException处理
     *
     * @param e 空指针异常
     * @return 统一响应结果
     */
    @ExceptionHandler(NullPointerException.class)
    public ResultVO<Void> handleNullPointerException(NullPointerException e) {
        log.error("空指针异常", e);
        return ResultVO.error(500, "系统内部错误");
    }

    /**
     * 其他未捕获的异常处理
     *
     * @param e 异常
     * @return 统一响应结果
     */
    @ExceptionHandler(Exception.class)
    public ResultVO<Void> handleException(Exception e) {
        log.error("系统异常", e);
        return ResultVO.error(500, "系统繁忙，请稍后重试");
    }
}