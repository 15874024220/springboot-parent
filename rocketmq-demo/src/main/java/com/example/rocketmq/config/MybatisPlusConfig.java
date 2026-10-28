package com.example.rocketmq.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.BlockAttackInnerInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.OptimisticLockerInnerInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.reflection.MetaObject;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.LocalDateTime;

/**
 * MyBatis-Plus配置类
 *
 * 功能说明：
 * 1. 配置分页插件：支持物理分页，防止全表查询
 * 2. 配置乐观锁插件：用于并发更新控制
 * 3. 配置防全表删除插件：防止误操作
 * 4. 配置自动填充：自动设置createTime、updateTime等字段
 *
 * @author example
 * 
 */
@Slf4j
@Configuration
@EnableTransactionManagement  // 启用Spring事务管理
@MapperScan("com.example.rocketmq.mapper")  // 扫描Mapper接口
public class MybatisPlusConfig {

    /**
     * MyBatis-Plus拦截器配置
     *
     * 拦截器执行顺序：多租户 -> 乐观锁 -> 防全表删除 -> 分页
     */
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        log.info("初始化MyBatis-Plus拦截器");

        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();

        // 1. 乐观锁插件：用于解决并发更新问题
        // 使用方式：在实体类的version字段上添加@Version注解
        // 更新时MyBatis-Plus会自动在WHERE条件中添加version判断，并自动+1
        interceptor.addInnerInterceptor(new OptimisticLockerInnerInterceptor());

        // 2. 防全表更新与删除插件：防止误操作导致数据丢失
        // 当执行不带where条件的update/delete时会抛出异常
        interceptor.addInnerInterceptor(new BlockAttackInnerInterceptor());

        // 3. 分页插件：实现物理分页，避免内存分页
        PaginationInnerInterceptor paginationInterceptor = new PaginationInnerInterceptor();
        paginationInterceptor.setDbType(DbType.MYSQL);      // 数据库类型
        paginationInterceptor.setMaxLimit(1000L);           // 单页最大限制
        paginationInterceptor.setOverflow(true);            // 溢出处理（超过最大页返回第一页）
        interceptor.addInnerInterceptor(paginationInterceptor);

        log.info("MyBatis-Plus拦截器初始化完成");
        return interceptor;
    }

    /**
     * 自动填充处理器
     *
     * 功能：在插入和更新时自动填充字段值
     * 使用方式：在实体类字段上添加 @TableField(fill = FieldFill.INSERT) 等注解
     */
    @Bean
    public MetaObjectHandler metaObjectHandler() {
        return new MetaObjectHandler() {

            /**
             * 插入时自动填充
             */
            @Override
            public void insertFill(MetaObject metaObject) {
                log.debug("执行插入自动填充");
                // 填充创建时间
                this.strictInsertFill(metaObject, "createTime", LocalDateTime.class, LocalDateTime.now());
                // 填充更新时间
                this.strictInsertFill(metaObject, "updateTime", LocalDateTime.class, LocalDateTime.now());
                // 填充逻辑删除标志（默认未删除）
                this.strictInsertFill(metaObject, "deleted", Integer.class, 0);
                // 填充乐观锁版本号（默认0）
                this.strictInsertFill(metaObject, "version", Integer.class, 0);
            }

            /**
             * 更新时自动填充
             */
            @Override
            public void updateFill(MetaObject metaObject) {
                log.debug("执行更新自动填充");
                // 自动更新更新时间
                this.strictUpdateFill(metaObject, "updateTime", LocalDateTime.class, LocalDateTime.now());
            }
        };
    }
}