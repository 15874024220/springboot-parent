package com.example.rocketmq.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.rocketmq.domain.entity.Order;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 订单Mapper接口
 *
 * 继承BaseMapper获得基础CRUD方法：
 * - insert: 插入
 * - deleteById: 根据ID删除
 * - updateById: 根据ID更新
 * - selectById: 根据ID查询
 * - selectList: 查询列表
 * - selectPage: 分页查询
 *
 * @author example
 * 
 */
@Mapper
public interface OrderMapper extends BaseMapper<Order> {

    /**
     * 根据订单号查询订单
     *
     * @param orderId 订单号
     * @return 订单实体
     */
    @Select("SELECT * FROM `order` WHERE order_id = #{orderId} AND deleted = 0")
    Order selectByOrderId(@Param("orderId") String orderId);

    /**
     * 更新订单状态
     *
     * @param orderId 订单号
     * @param status 新状态
     * @return 影响行数
     */
    @Update("UPDATE `order` SET status = #{status}, update_time = NOW() " +
            "WHERE order_id = #{orderId} AND deleted = 0")
    int updateStatus(@Param("orderId") String orderId,
                     @Param("status") Integer status);

    /**
     * 使用乐观锁更新订单状态
     *
     * @param orderId 订单号
     * @param status 新状态
     * @param version 当前版本号
     * @return 影响行数
     */
    @Update("UPDATE `order` SET status = #{status}, version = version + 1, update_time = NOW() " +
            "WHERE order_id = #{orderId} AND version = #{version} AND deleted = 0")
    int updateStatusWithVersion(@Param("orderId") String orderId,
                                @Param("status") Integer status,
                                @Param("version") Integer version);
}