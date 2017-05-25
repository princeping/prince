package com.prince.guide;

import org.junit.Test;

import com.prince.guide.util.FileUtil;

public class FileUtilTest {

	@Test
	public void getFileNumberTest(){
		int num = FileUtil.getFileNumber("D:/有道");
		System.out.println(num);
	}
	
	@Test
	public void delFolderTest(){
		boolean flag = FileUtil.delFolder("E:/新建文件夹");
		System.out.println(flag);
	}
	
	@Test
	public void delFileTest(){
		boolean flag = FileUtil.delFile("E:/新建文件夹/aaa.txt");
		System.out.println(flag);
	}
	
	@Test
	public void delAllFileTest(){
		boolean flag = FileUtil.delAllFile("E:/新建文件夹");
		System.out.println(flag);
	}
}
