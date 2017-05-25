package com.prince.guide.util.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis通用工具类接口
 * Created by zhangxin on 2016/11/17.
 */
public interface RedisUtil {

    /**
     * 判断一个key值是否存在
     * @param key 要判断的key
     * @return 如果存在返回true，否则返回false
     */
    boolean hasKey(String key);

    /**
     * 获取所有的key
     * @return 获取到的所有key
     */
    Set<String> getKeys();

    /**
     * 添加一个String
     * @param key key
     * @param value value
     */
    void addString(String key, String value);

    /**
     * 添加一个String，并设置有效时间
     * @param key key
     * @param value value
     * @param time 有效时间，单位秒
     */
    void addString(String key, String value, long time);

    /**
     * 获取一个String类型数据
     * @param key key
     * @return 获取到的数据
     */
    String getString(String key);

    /**
     * 向list中添加一条数据，不覆盖原数据（value值允许重复）
     * @param key key
     * @param value value
     * @param leftOrRight 插入方向，传空或left从左插入，传right从右插入
     */
    void addListForOne(String key, String value, String leftOrRight);

    /**
     * 向list中添加一组数据，不覆盖原数据（value值允许重复）
     * @param key key
     * @param value value
     * @param leftOrRight 插入方向，传空或left从左插入，传right从右插入
     */
    void addList(String key, Collection<String> value, String leftOrRight);

    /**
     * 替换list中的一条数据
     * @param key key
     * @param cnt 数据位置，下标从0开始
     * @param value 替换后的数据
     */
    void setListForOne(String key, Long cnt, String value);

    /**
     * 获取一个list中全部数据
     * @param key key
     * @return 获取到的list
     */
    List<String> getList(String key);

    /**
     * 向set中添加一组数据（value值不允许重复，重复则覆盖）
     * @param key key
     * @param value value
     */
    void addSet(String key, String... value);

    /**
     * 获取一个Set中全部数据
     * @param key key
     * @return 获取到的set
     */
    Set<String> getSet(String key);

    /**
     * 向有序set中添加一条数据（value值重复位置不同则覆盖，value值不同位置相同则添加）
     * @param key key
     * @param value value
     * @param number 数据位置，下标从0开始
     */
    void addZSet(String key, String value, double number);

    /**
     * 有序set中添加一组数据（value值重复位置不同则覆盖，value值不同位置相同则添加）
     * @param key key
     * @param values value值集合
     * @param numbers 位置集合
     */
    void addZSet(String key, String[] values, double[] numbers);

    /**
     * 获取一个ZSet中全部数据
     * @param key key
     * @return 获取到的有序set
     */
    Set<Object> getZSet(String key);

    /**
     * 向map中添加一条数据，不覆盖原数据（mapkey值不能重复，value值允许重复）
     * @param key key
     * @param value value
     */
    void addHash(String key, String mapkey, String value);

    /**
     * 向map中添加一组数据，不覆盖原数据（mapkey值不能重复，value值允许重复）
     * @param key key
     * @param value value
     */
    void addHash(String key, Map<String,String> value);

    /**
     * 获取一个Hash中全部数据
     * @param key key
     * @return 获取到的map
     */
    Map<String, String> getHash(String key);

    /**
     * 获取一个Hash中一条数据
     * @param key key
     * @return 获取到的数据
     */
    Object getHash(String key, String hashkey);

    /**
     * 删除一条数据
     * @param key key
     */
    void delete(String key);

    /**
     * 删除一批数据
     * @param keys key集合
     */
    void delete(List<String> keys);

    /**
     * 清空全部数据
     */
    void deleteDb();
}
