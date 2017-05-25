package com.prince.guide.dao;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

/**
 * guide持久化层接口
 * @author princeping
 */
@Repository
public interface GuideDao {

    /**
     * 更新医院统计数据
     * @param map
     */
    void updateHospital(Map<String, Object> map);

    /**
     * 查询医院下疾病统计数据是否存在
     * @param list 联合id集合
     * @return 存在的联合id
     */
    List<String> queryHdisease(List<String> list);

    /**
     * 批量新增医院下疾病统计数据
     * @param list
     */
    void insertHdisease(List<Map<String,Object>> list);

    /**
     * 批量更新医院下疾病统计数据
     * @param list
     */
    void updateHdisease(List<Map<String,Object>> list);

    /**
     * 更新医院统计数据
     * @param map
     */
    void updateDisease(Map<String, Object> map);
}
