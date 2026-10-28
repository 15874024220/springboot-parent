package com.example.rocketmq.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 订单实体类
 *
 * 使用MyBatis-Plus注解实现ORM映射
 *
 * @author example
 * 
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("`order`")     // 指定表名（反引号避免MySQL关键字冲突）
public class Order implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 主键ID（自增）
     * IdType.AUTO：使用数据库自增主键
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 订单号（业务唯一键）
     * 用于幂等性校验和业务查询
     */
    @TableField("order_id")
    private String orderId;

    /**
     * 订单状态
     * 1-创建 2-支付 3-完成 4-取消
     * @see com.example.rocketmq.domain.enums.OrderStatusEnum
     */
    @TableField("status")
    private Integer status;

    /**
     * 订单金额（分）
     * 使用分单位避免浮点数精度问题
     */
    @TableField("amount")
    private Long amount;

    /**
     * 用户ID
     */
    @TableField("user_id")
    private String userId;

    /**
     * 订单内容（JSON格式）
     * 存储订单的商品信息、地址等扩展信息
     */
    @TableField("content")
    private String content;

    /**
     * 乐观锁版本号
     * @Version注解：更新时自动检查版本号，防止并发覆盖
     * 使用方式：更新时MyBatis-Plus会自动在WHERE条件中添加version判断
     */
    @Version
    @TableField("version")
    private Integer version;

    /**
     * 逻辑删除标识
     * @TableLogic注解：删除时更新此字段而非物理删除
     * 0-未删除，1-已删除
     */
    @TableLogic
    @TableField("deleted")
    private Integer deleted;

    /**
     * 创建时间
     * fill = FieldFill.INSERT：插入时自动填充
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    /**
     * 更新时间
     * fill = FieldFill.INSERT_UPDATE：插入和更新时自动填充
     */
    @TableField(value = "update_time", fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
}