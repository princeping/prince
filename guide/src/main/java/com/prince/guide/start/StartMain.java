package com.prince.guide.start;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prince.guide.hospital.HospitalJob;
import com.prince.guide.medical.MedicalGuidanceJob;
import com.prince.guide.service.GuideService;
import com.prince.guide.util.FileUtil;
import com.prince.guide.util.JobControlUtil;
import com.prince.guide.util.hbase.HbaseUtil;
import com.prince.guide.util.hbase.impl.HbaseUtilImpl;
import com.prince.guide.util.hdfs.HdfsUtil;
import com.prince.guide.util.hdfs.impl.HdfsUtilImpl;
import com.prince.guide.util.redis.RedisUtil;

import jodd.datetime.JDateTime;

/**
 * job总管
 * @author princeping
 */
public class StartMain {

	private static Logger logger = LogManager.getLogger("StartMain");
	
	public static void main(String[] args){
		
		long starttime = System.currentTimeMillis();
		logger.info("StartMain开始运行");
		
		StartMain startMain = new StartMain();
		HdfsUtil hdfsUtil = new HdfsUtilImpl();
		HbaseUtil hbaseUtil = new HbaseUtilImpl();
		RedisUtil redisUtil = (RedisUtil)ConfigFactory.getInstance().getContext().getBean("redisUtil");
		try {
			if(!startMain.checkPaths(hdfsUtil, hbaseUtil)){
				return;
			}
			logger.info("加载数据到redis开始");
			long redistime = System.currentTimeMillis();
			startMain.LoadDataToRedis(hbaseUtil, redisUtil);
			logger.info("加载数据到redis结束，耗时：" + (System.currentTimeMillis()-redistime)/1000);
			boolean flag = startMain.runJob();
			if(flag){
				long starttime2 = System.currentTimeMillis();
				logger.info("运行结束后续操作开始");
				startMain.downLoadHistory(hdfsUtil);
				startMain.copyDataToHistory(hdfsUtil);
				logger.info("运行结束后续操作耗时：" + (System.currentTimeMillis()-starttime2)/1000);
				logger.info("持久化数据到hbase开始");
				long hbasetime = System.currentTimeMillis();
				startMain.loadDataToHbase(hbaseUtil, redisUtil);
				logger.info("持久化数据到hbase耗时：" + (System.currentTimeMillis()-hbasetime)/1000);
				logger.info("持久化数据到mysql开始");
				long writeDataToSqltime = System.currentTimeMillis();
				startMain.writeDataToSql();
				logger.info("持久化数据到mysql耗时：" + (System.currentTimeMillis()-writeDataToSqltime)/1000);
			}
			logger.info("StartMain运行结束，运行耗时：" + (System.currentTimeMillis()-starttime)/1000);
		} catch (IOException | InterruptedException e) {
			logger.error("StartMain.main", e);
		}
	}
	
	/**
	 * 第一步：检查hdfs和hbase
	 * @return 检查通过返回true，否则返回false
	 * @throws IOException 
	 */
	private boolean checkPaths(HdfsUtil hdfsUtil, HbaseUtil hbaseUtil) throws IOException{
		if(!checkTodayData(hdfsUtil))
			return false;
		long starttime1 = System.currentTimeMillis();
		logger.info("hdfs和hbase检查开始");
		checkHdfsPutOutPath(hdfsUtil);
		checkHbase(hbaseUtil);
		logger.info("hdfs和hbase检查结束，耗时：" + (System.currentTimeMillis()-starttime1)/1000);
		return true;
	}
	
	/**
	 * 第二步：检查并上传本地文件到hdfs
	 * 检查hdfs输入文件路径，存在则清空，不存在则创建
	 * @param hdfsUtil hdfs工具类
	 * @return 不存在返回false，否则返回true
	 * @throws IOException 
	 */
	private boolean checkTodayData(HdfsUtil hdfsUtil) throws IOException{
		long starttime4 = System.currentTimeMillis();
		logger.info("输入文件检查开始");
		//上传hdfs前检查，检查本地上传文件是否存在
		if(FileUtil.isNotNullForPaths(ConfigFactory.getInstance().getFilepaths())){
			logger.error("输入文件不存在");
			return false;
		}
		//上传hdfs前检查，检查目标路径是否已存在，存在则清空
		String temphdfspath = ConfigFactory.getInstance().getHdfspaths().get("todaypath");
		if(hdfsUtil.existsFiles(temphdfspath)){
			hdfsUtil.deletePath(temphdfspath);
		}
		hdfsUtil.createFolder(temphdfspath);
		//上传本地文件到hdfs
		for(String path : ConfigFactory.getInstance().getFilepaths()){
			hdfsUtil.localFileUploadHDFS(path, temphdfspath);
		}
		logger.info("输入文件检查结束，耗时：" + (System.currentTimeMillis()-starttime4)/1000);
		return true;
	}
	
	/**
	 * 第三步：清空MapReduce的输出路径
	 * @param hdfsUtil hdfs工具类
	 * @throws IOException 
	 */
	private void checkHdfsPutOutPath(HdfsUtil hdfsUtil) throws IOException{
		//返回此映射中包含的键的 set 视图
		Set<String> keys = ConfigFactory.getInstance().getHdfspaths().keySet();
		for(String key : keys){
			if("todaypath".equals(key)){
				continue;
			}//TODO
			String path = ConfigFactory.getInstance().getHdfspaths().get(key);
			if(hdfsUtil.existsFiles(path)){
				hdfsUtil.deletePath(path);
			}
		}
	}
	
	/**
	 * 第四步：检查目标hbase表是否存在，不存在则创建
	 * @throws IOException 
	 */
	private void checkHbase(HbaseUtil hbaseUtil) throws IOException{
		if(!hbaseUtil.existsTable("guide")){
			String[] columnFamilys = new String[]{"hospital", "disease", "hdisease"};
			hbaseUtil.createTable("guide", columnFamilys);
		}
	}
	
	/**
	 * 第六步：下载hdfs解析结果到本地
	 * @param hdfsUtil hdfs工具类
	 * @throws IOException
	 */
	private void downLoadHistory(HdfsUtil hdfsUtil) throws IOException{
		if(FileUtil.delAllFile(ConfigFactory.getInstance().getResultData())){
			
			if(hdfsUtil.existsFiles(ConfigFactory.getInstance().getHdfspaths().get("hospitaloutput"))){
				hdfsUtil.hdfsDownLocal(ConfigFactory.getInstance().getResultData(),
						ConfigFactory.getInstance().getHdfspaths().get("hospitaloutput"));
			}
//			if(hdfsUtil.existsFiles(ConfigFactory.getInstance().getHdfspaths().get("diseaseoutput"))){
//				hdfsUtil.hdfsDownLocal(ConfigFactory.getInstance().getResultData(),
//						ConfigFactory.getInstance().getHdfspaths().get("diseaseoutput"));
//			}
//			if(hdfsUtil.existsFiles(ConfigFactory.getInstance().getHdfspaths().get("hdiseaseoutput"))){
//				hdfsUtil.hdfsDownLocal(ConfigFactory.getInstance().getResultData(),
//						ConfigFactory.getInstance().getHdfspaths().get("hdiseaseoutput"));
//			}
		}
	}
	
	/**
	 * 移动当日文件到历史库
	 * @param hdfsUtil hdfs工具类
	 * @throws IOException
	 */
	private void copyDataToHistory(HdfsUtil hdfsUtil) throws IOException{
		JDateTime jDateTime = new JDateTime();
		String path = ConfigFactory.getInstance().getHistoryData()+jDateTime.toString("YYYY-MM-DD");
		hdfsUtil.createFolder(path);
		hdfsUtil.copyFiles(ConfigFactory.getInstance().getHdfspaths().get("hospitaloutput"),path);
	}
	
	/**
	 * 运行所有的mr
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	private boolean runJob() throws IOException, InterruptedException{
		
		long starttime3 = System.currentTimeMillis();
		logger.info("Job运行开始");
		
		MedicalGuidanceJob medicalGuidanceJob = new MedicalGuidanceJob();
		HospitalJob hospitalJob = new HospitalJob();
		
		Job job1 = medicalGuidanceJob.getJob();
		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
		controlledJob1.setJob(job1);
		
		Job job2 = hospitalJob.getJob();
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		controlledJob2.setJob(job2);
		controlledJob2.addDependingJob(controlledJob1);
		
		boolean flag = JobControlUtil.manageJob("guidejob", controlledJob1, controlledJob2);
		logger.info("Job运行结束，运行耗时：" + (System.currentTimeMillis()-starttime3)/1000);
		return flag;
	}
	
	/**
	 * 第五步：从hbase读取所有数据，存入redis
	 * @param hbaseUtil hbase工具类
	 * @param redisUtil redis工具类
	 * @throws IOException
	 */
	private void LoadDataToRedis(HbaseUtil hbaseUtil, RedisUtil redisUtil) throws IOException{
		String rowkey = null;
		int size = 10000;
		while(true){
			List<List<Cell>> datalist = hbaseUtil.getPageData("guide", rowkey, size);
			if(datalist==null || datalist.size()<1 ){
				break;
			}
			for(List<Cell> templist : datalist){
				String key = Bytes.toString(CellUtil.cloneRow(templist.get(0)));
				String value = Bytes.toString(CellUtil.cloneValue(templist.get(0)));
				redisUtil.addString(key, value);
			}
			if(datalist.size()<size){
				break;
			}
			rowkey = Bytes.toString(CellUtil.cloneRow(datalist.get(datalist.size()-1).get(0)));
		}
	}
	
	/**
	 * 持久化数据到hbase
	 * @param hbaseUtil
	 * @param redisUtil
	 * @throws IOException
	 */
	private void loadDataToHbase(HbaseUtil hbaseUtil, RedisUtil redisUtil) throws IOException{
		Set<String> keys = redisUtil.getKeys();
		if(keys!=null&&keys.size()>0){
			List<String> rows = new ArrayList<>(1000);
			List<String> familys = new ArrayList<>(1000);
			List<String> columns = new ArrayList<>(1000);
			List<String> values = new ArrayList<>(1000);
			for(String key : keys){
				rows.add(key);
				if(key.indexOf("hos")>-1)
					familys.add("hospital");
				else if(key.indexOf("hdis")>-1)
					familys.add("hdisease");
				else
					familys.add("disease");
				columns.add(null);
				values.add(redisUtil.getString(key));
				if(rows.size()==1000){
					writeToHbase(hbaseUtil, rows, familys, columns, values);
				}
			}
			if(rows.size()>0){
				writeToHbase(hbaseUtil, rows, familys, columns, values);
			}
			redisUtil.deleteDb();
		}
	}
	
	/**
	 * 写入数据到hbase，并清空集合
	 * @param hbaseUtil  hbase工具类
	 * @param rows 行键集合
	 * @param familys 列簇集合
	 * @param columns 子列集合
	 * @param values 列值集合
	 * @throws IOException
	 */
	private void writeToHbase(HbaseUtil hbaseUtil, List<String> rows,
			List<String> familys, List<String> columns, List<String> values) throws IOException{
		hbaseUtil.insertRow("guide", rows, familys, columns, values);
		rows.clear();
		familys.clear();
		columns.clear();
		values.clear();
	}
	
	private void writeDataToSql() throws IOException{
		GuideService guideService = (GuideService)ConfigFactory.getInstance().getContext().getBean("guideService");
		//组装写入到mysql的本地文件路径
		String hospath = ConfigFactory.getInstance().getResultData() + File.separator + "hospitaloutput" + File.separator + "part-r-00000";
		List<String> hosDataList = FileUtil.readFile(hospath);
		if(hosDataList!=null && hosDataList.size()>0){
			for(String hosData : hosDataList){
				guideService.updateHospital(hosData);
			}
		}
		String dispath = ConfigFactory.getInstance().getResultData() + File.separator + "hospitaloutput" + File.separator + "part-r-00002";
		List<String> disDataList = FileUtil.readFile(dispath);
		if(disDataList!=null && disDataList.size()>0){
			for(String disData : disDataList){
				guideService.updateDisease(disData);
			}
		}
		String hdispath = ConfigFactory.getInstance().getResultData() + File.separator + "hospitaloutput" + File.separator + "part-r-00001";
		List<String> hdisDataList = FileUtil.readFile(hdispath);
				guideService.updateHdisease(hdisDataList);
	}
}
