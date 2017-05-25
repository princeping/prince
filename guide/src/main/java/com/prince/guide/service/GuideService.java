package com.prince.guide.service;

import java.util.List;

import org.springframework.stereotype.Repository;

/**
 * guideService层接口
 * @author princeping
 */
@Repository
public interface GuideService {

    /**
     * 更新医院统计信息
     * @param message 解析结果原始信息
     */
    void updateHospital(String message);

    /**
     * 更新医院下疾病统计数据
     * 已有数据则更新，没有数据则新增
     * @param list 解析结果原始信息集合
     */
    void updateHdisease(List<String> list);

    /**
     * 更新疾病统计信息
     * @param message 解析结果原始信息
     */
    void updateDisease(String message);
}