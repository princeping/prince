package com.prince.guide.util;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobControlUtil {

	private static Logger logger = LogManager.getLogger("JobControlUtil");
	
	/**
	 * 链式job管理工具
	 * @param jobname job名称
	 * @param jobs job集合
	 * @return 全部完成返回true，否则返回false
	 * @throws InterruptedException
	 */
	public static boolean manageJob(String jobname, ControlledJob... jobs) throws InterruptedException{
		long starttime = System.currentTimeMillis();
		JobControl jobControl = new JobControl("JobControlUtil");
		for(ControlledJob job : jobs){
			jobControl.addJob(job);
		}
		Thread thread = new Thread(jobControl);
		thread.start();
		boolean flag = false;
		int cnt=0;
		long onetime = System.currentTimeMillis();
		while(true){
			if(jobControl.getSuccessfulJobList().size()>cnt){
				logger.info(jobname + "Job运行完成，单一Job耗时：" + (System.currentTimeMillis()-onetime)/1000);
				logger.info(jobControl.getSuccessfulJobList().get(cnt));
				cnt++;
				onetime = System.currentTimeMillis();
			}
			if(jobControl.allFinished()){
				long time = System.currentTimeMillis()-starttime;
				logger.info(jobname + "运行顺利完成，总耗时：" + time+"毫秒，" +time/1000+"秒，" +time/1000/60 +"分");
				logger.info("—————— 成功Job列表 ——————");
				for(ControlledJob tempjob : jobControl.getSuccessfulJobList()){
					logger.info(tempjob);
				}
				logger.info("——————————————————");
				jobControl.stop();
				flag = true;
				break;
			}
			if(jobControl.getFailedJobList().size()>0){
				long time = System.currentTimeMillis()-starttime;
				logger.info(jobname + "运行失败，总耗时：" + time+"毫秒，" +time/1000+"秒，" +time/1000/60 +"分");
				logger.info("—————— 失败Job列表 ——————");
				for(ControlledJob tempjob : jobControl.getFailedJobList()){
					logger.info(tempjob);
				}
				logger.info("——————————————————");
				jobControl.stop();
				flag = false;
				break;
			}
			TimeUnit.MILLISECONDS.sleep(2*1000);
		}
		return flag;
	}
}
