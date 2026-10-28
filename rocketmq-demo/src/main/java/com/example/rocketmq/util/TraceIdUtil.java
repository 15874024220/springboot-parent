package com.example.rocketmq.util;

import cn.hutool.core.util.IdUtil;
import org.slf4j.MDC;

/**
 * 链路追踪工具类
 *
 * 功能：
 * 1. 生成唯一的TraceId
 * 2. 在MDC中存储TraceId
 * 3. 支持日志中打印TraceId，便于问题排查
 *
 * 使用方式：
 * - 在请求入口处调用 setTraceId()
 * - 在日志配置中使用 %X{traceId} 打印TraceId
 *
 * @author example
 * 
 */
public class TraceIdUtil {

    private static final String TRACE_ID_KEY = "traceId";

    /**
     * 获取或创建TraceId
     *
     * @return TraceId
     */
    public static String getOrCreateTraceId() {
        String traceId = MDC.get(TRACE_ID_KEY);
        if (traceId == null || traceId.trim().isEmpty()) {
            traceId = generateTraceId();
            MDC.put(TRACE_ID_KEY, traceId);
        }
        return traceId;
    }

    /**
     * 设置TraceId
     *
     * @param traceId TraceId
     */
    public static void setTraceId(String traceId) {
        if (traceId != null && !traceId.trim().isEmpty()) {
            MDC.put(TRACE_ID_KEY, traceId);
        } else {
            MDC.put(TRACE_ID_KEY, generateTraceId());
        }
    }

    /**
     * 清除TraceId
     */
    public static void clearTraceId() {
        MDC.remove(TRACE_ID_KEY);
    }

    /**
     * 生成TraceId
     *
     * @return 唯一的TraceId
     */
    private static String generateTraceId() {
        return IdUtil.fastSimpleUUID();
    }
}