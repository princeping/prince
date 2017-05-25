package com.prince.guide.util.redis.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import com.prince.guide.util.redis.RedisUtil;

import jodd.util.StringUtil;

@Repository(value = "redisUtil")
public class RedisUtilImpl implements RedisUtil {

    private RedisTemplate<String, Object> redisTemplate;

    @Resource
    @Qualifier("redisTemplate")
    public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean hasKey(String key){
        return redisTemplate.hasKey(key);
    }

    @Override
    public Set<String> getKeys(){
        return redisTemplate.keys("*");
    }

    @Override
    public void addString(String key, String value) {
        redisTemplate.opsForValue().set(key,value);
    }

    @Override
    public void addString(String key, String value, long time) {
        redisTemplate.opsForValue().set(key,value,time, TimeUnit.SECONDS);
    }

    @Override
    public String getString(String key) {
        Object object = redisTemplate.opsForValue().get(key);
        if(object!=null)
            return String.valueOf(object);
        else
            return null;
    }

    @Override
    public void addListForOne(String key, String value, String leftOrRight) {
        if(StringUtil.isNotEmpty(leftOrRight)&&"right".equals(leftOrRight)){
            redisTemplate.opsForList().rightPush(key,value);
        }else{
            redisTemplate.opsForList().leftPush(key,value);
        }
    }

    @Override
    public void addList(String key, Collection<String> value, String leftOrRight) {
        if(StringUtil.isNotEmpty(leftOrRight)&&"right".equals(leftOrRight)){
            redisTemplate.opsForList().rightPushAll(key,value.toArray());
        }else{
            redisTemplate.opsForList().leftPushAll(key,value.toArray());
        }
    }

    @Override
    public void setListForOne(String key, Long cnt, String value) {
        redisTemplate.opsForList().set(key,cnt,value);
    }

    @Override
    public List<String> getList(String key) {
        List<Object> listdata = redisTemplate.opsForList().range(key,0,-1);
        if(listdata==null||listdata.size()<1)
            return null;
        List<String> returnValue = new ArrayList<>(listdata.size());
        for(Object object : listdata) {
            returnValue.add(String.valueOf(object));
        }
        return returnValue;
    }

    @Override
    public void addSet(String key, String... value) {
        redisTemplate.opsForSet().add(key,value);
    }

    @Override
    public Set<String> getSet(String key) {
        Set<Object> setdata = redisTemplate.opsForSet().members(key);
        Set<String> returnValue = null;
        if(setdata!=null&&setdata.size()>0){
            returnValue = new HashSet<>(setdata.size());
            for(Object object : setdata){
                returnValue.add(String.valueOf(object));
            }
        }
        return returnValue;
    }

    @Override
    public void addZSet(String key, String value, double number) {
        redisTemplate.opsForZSet().add(key,value,number);
    }

    @Override
    public void addZSet(String key, String[] values, double[] numbers) {
        Set<ZSetOperations.TypedTuple<Object>> dataset = new HashSet<>(values.length);
        for(int cnt=0; cnt<values.length; cnt++){
            dataset.add(new DefaultTypedTuple<>(values[cnt],numbers[cnt]));
        }
        redisTemplate.opsForZSet().add(key, dataset);
    }

    @Override
    public Set<Object> getZSet(String key) {
        return redisTemplate.opsForZSet().range(key,0,-1);
    }

    @Override
    public void addHash(String key, String mapkey, String value) {
        redisTemplate.opsForHash().put(key,mapkey,value);
    }

    @Override
    public void addHash(String key, Map<String, String> value) {
        redisTemplate.opsForHash().putAll(key,value);
    }

    @Override
    public Map<String, String> getHash(String key) {
        Map<Object,Object> mapdata = redisTemplate.opsForHash().entries(key);
        if(mapdata==null||mapdata.size()<1)
            return null;
        Set<Object> keys = mapdata.keySet();
        Map<String,String> returnValue = new HashMap<>(keys.size());
        for(Object mapkey : keys){
            returnValue.put(String.valueOf(mapkey),String.valueOf(mapdata.get(mapkey)));
        }
        return returnValue;
    }

    @Override
    public Object getHash(String key, String hashkey) {
        return redisTemplate.opsForHash().get(key, hashkey);
    }

    @Override
    public void delete(String key) {
        redisTemplate.delete(key);
    }

    @Override
    public void delete(List<String> keys) {
        redisTemplate.delete(keys);
    }

    @Override
    public void deleteDb() {
        redisTemplate.getConnectionFactory().getConnection().flushAll();
    }
}