package com.prince.guide.hdfs;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.prince.guide.start.ConfigFactory;
import com.prince.guide.util.hdfs.HdfsUtil;
import com.prince.guide.util.hdfs.impl.HdfsUtilImpl;

/**
 * hdfs工具类测试用例
 * @author princeping
 */
public class HdfsUtilTest {

	private Logger logger = LogManager.getLogger("HdfsUtilTest");
	
	private HdfsUtil hdfsUtil = new HdfsUtilImpl();
	
	//判断文件或文件夹是否存在
	@Test
	public void existsFilesTest() {
		try {
			System.out.println(hdfsUtil.existsFiles("/prince/historydata2016-12-06/hospitaloutput/part-r-00000"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.existsFilesTest", e);
		}
	}
	
	//创建文件夹
	@Test
	public void createFolderTest(){
		try {
			System.out.println(hdfsUtil.createFolder("/prince/MedicalGuidance"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.createFolderTest", e);
		}
	}
	
	//创建文件
	@Test
	public void createFileTest(){
		try {
			System.out.println(hdfsUtil.createFile("/demo/logs/error.log"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.createFileTest", e);
		}
	}
//	
//	@Test
//	public void writeFileTest(){
//		try {
//			System.out.println(hdfsUtil.writeFile("/newFolder1111/we.txt", "2014"));
//		} catch (IOException e) {
//			logger.error("HdfsUtilTest.writeFileTest", e);
//		}
//	}
	
	//读取文件
	@Test
	public void readFileTest(){
		try {
			System.out.println(hdfsUtil.readFile("/demo"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.readFileTest", e);
		}
	}
	
	//上传文件
	@Test
	public void localFileUploadHDFSTest(){
		try {
			System.out.println(hdfsUtil.localFileUploadHDFS("E:/guide/resultdata/hospitaloutput/part-r-00000.txt", "/prince"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.localFileUploadHDFSTest", e);
		}
	}
	
	//上传文件夹
	@Test
	public void localFolderUploadHDFSTest() {
		try {
			System.out.println(hdfsUtil.localFolderUploadHDFS("E:/Sort", "/prince"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.localFolderUploadHDFSTest", e);
		}

	}
	
	//下载
	@Test
	public void hdfsDownLocalTest(){
		try {
			System.out.println(hdfsUtil.hdfsDownLocal("/prince/hospitaloutput", "E:/guide/resultdata"));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.hdfsDownLocalTest", e);
		}
	}
	
	//删除
	@Test
	public void deletePath(){
		try {
			System.out.println(hdfsUtil.deletePath(ConfigFactory.getInstance().getHdfspaths().get("hospitaloutput")));
		} catch (IOException e) {
			logger.error("HdfsUtilTest.deletePath", e);
		}
	}
}
