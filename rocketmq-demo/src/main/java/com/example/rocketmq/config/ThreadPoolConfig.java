package com.example.rocketmq.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池配置类
 *
 * 功能说明：
 * 1. 异步任务线程池：用于@Async异步方法
 * 2. 业务处理线程池：用于耗时业务逻辑
 *
 * @author example
 * 
 */
@Slf4j
@Configuration
@EnableAsync  // 启用Spring异步方法执行支持
public class ThreadPoolConfig {

    /**
     * 通用异步任务线程池
     *
     * 适用场景：
     * - @Async注解的异步方法
     * - 异步通知、日志记录等非核心业务
     *
     * @return Executor
     */
    @Bean(name = "asyncExecutor")
    public Executor asyncExecutor() {
        log.info("初始化异步任务线程池");

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // CPU核心数
        int processors = Runtime.getRuntime().availableProcessors();

        // 核心线程数：CPU核心数 * 2
        int corePoolSize = processors * 2;
        executor.setCorePoolSize(corePoolSize);

        // 最大线程数：核心线程数 * 2
        executor.setMaxPoolSize(corePoolSize * 2);

        // 队列容量：等待执行的任务队列大小
        executor.setQueueCapacity(500);

        // 线程存活时间：空闲线程的存活时间（秒）
        executor.setKeepAliveSeconds(60);

        // 线程名称前缀（便于问题排查）
        executor.setThreadNamePrefix("async-exec-");

        // 拒绝策略：当队列满且线程数达到最大时的处理策略
        // CallerRunsPolicy：由调用线程处理（降低新任务提交速度）
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        // 等待所有任务完成后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);

        // 关闭时等待任务完成的时间（秒）
        executor.setAwaitTerminationSeconds(60);

        executor.initialize();

        log.info("异步任务线程池初始化完成，核心线程数: {}, 最大线程数: {}",
                corePoolSize, corePoolSize * 2);

        return executor;
    }

    /**
     * 业务处理专用线程池
     *
     * 适用场景：
     * - 耗时较长的业务逻辑处理
     * - 批量数据处理
     *
     * @return Executor
     */
    @Bean(name = "businessExecutor")
    public Executor businessExecutor() {
        log.info("初始化业务处理线程池");

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(500);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("business-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);

        executor.initialize();

        log.info("业务处理线程池初始化完成");
        return executor;
    }
}