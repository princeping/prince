package com.prince.guide;

import javax.annotation.Resource;

import org.junit.Test;

import com.prince.guide.service.GuideService;

public class GuideServiceTest extends BaseJunit4Test{

	@Resource
	private GuideService guideService;
	
	@Test
	public void updateHospitalTest(){
		guideService.updateHospital("hos280724410105251031	1	1	1	1	1	1	1	1	1	1	1");
	}
	
	@Test
	public void updateDiseaseTest(){
		guideService.updateDisease("disA00.000	1	1	1	1	1	1	1	1	1	1	1");
	}
}
