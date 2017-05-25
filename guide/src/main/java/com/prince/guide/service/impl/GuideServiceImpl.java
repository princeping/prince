package com.prince.guide.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.prince.guide.dao.GuideDao;
import com.prince.guide.service.GuideService;

import jodd.util.StringUtil;

/**
 * guideService层实现类
 * @author princeping
 */
@Service(value = "guideService")
public class GuideServiceImpl implements GuideService{

	@Resource
	private GuideDao guideDao;
	
	@Override
	public void updateHospital(String message) {
		if(StringUtil.isNotEmpty(message)){
			String[] array = StringUtil.split(message, "\t");
			if(array!=null&&array.length==12){
				Map<String, Object> map = getMapParam(array);
				map.put("hospitalid", array[0].substring(3));
				guideDao.updateHospital(map);
			}
		}
	}

	@Override
	public void updateHdisease(List<String> list) {
		if(list==null || list.size()<1)
			return;
		List<String> margeids = new ArrayList<>(list.size());
		List<Map<String, Object>> listmap = getListParam(list, margeids);
		List<String> updateids = queryHdisease(margeids);
		List<Map<String, Object>> insertlistMap = new ArrayList<>();
		splitList(listmap, insertlistMap, updateids);
		guideDao.insertHdisease(insertlistMap);
		guideDao.updateHdisease(listmap);
	}
	
	private List<Map<String, Object>> getListParam(List<String> list, List<String> margeids){
		List<Map<String, Object>> listmap = new ArrayList<>();
		for(String str : list){
			String[] array = StringUtil.split(str, "\t");
			if(array!=null&&array.length==12){
				Map<String, Object> map = getMapParam(array);
				String margeid = array[0].substring(4);
				String[] ids = StringUtil.split(margeid, "^^");
				map.put("hospitalid", ids[0]);
				map.put("diseaseid", ids[1]);
				map.put("margeid", margeid);
				margeids.add(margeid);
				listmap.add(map);
			}
		}
		return listmap;
	}

	/**
	 * 组装dao层参数
	 * @param array
	 * @return
	 */
	private Map<String, Object> getMapParam(String[] array){
		Map<String, Object> returnMap = new HashMap<>();
		returnMap.put("hcount", Double.valueOf(array[1]).intValue());
		returnMap.put("hallcost", Double.valueOf(array[2]));
		returnMap.put("hreimbursecost",Double.valueOf(array[3]));
	    returnMap.put("hday",Double.valueOf(array[5]));
	    returnMap.put("hrecovery",Double.valueOf(array[6]));
	    returnMap.put("ocount",Double.valueOf(array[7]).intValue());
	    returnMap.put("ohallcost",Double.valueOf(array[8]));
	    returnMap.put("ohreimbursecost",Double.valueOf(array[9]));
	    returnMap.put("ohrecovery",Double.valueOf(array[11]));
	    return returnMap;
	}
	
	private List<String> queryHdisease(List<String> list){
		return guideDao.queryHdisease(list);
	}
	
	/**
	 * 拆分list，拆分后，原始list变为需要更新的数据，insertlistMap为需要新增的数据
	 * @param listMap 原始数据集合
	 * @param insertlistMap 新增数据集合
	 * @param updateids 已存在集合
	 */
	private void splitList(List<Map<String, Object>> listMap,
			List<Map<String, Object>> insertlistMap, List<String> updateids){
		Iterator<Map<String, Object>> iterator = listMap.iterator();
		while(iterator.hasNext()){
			Map<String, Object> tempMap = iterator.next();
			String margeid = String.valueOf(tempMap.get("margeid"));
			if(!updateids.contains(margeid)){
				insertlistMap.add(tempMap);
				iterator.remove();
			}
		}
	}

	@Override
	public void updateDisease(String message) {
		if(StringUtil.isNotEmpty(message)){
			String[] array = StringUtil.split(message, "\t");
			if(array!=null&&array.length==12){
				Map<String, Object> map = getMapParam(array);
				map.put("diseaseid", array[0].substring(3));
				guideDao.updateDisease(map);
			}
		}
	}
}
