package com.prince.guide.hdisease;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prince.guide.hospital.HospitalReduce;
import com.prince.guide.start.ConfigFactory;

public class HdiseaseJob {

	private static Logger logger = LogManager.getLogger("HdiseaseJob");
	
	public Job getJob() throws IOException {
		Job job = Job.getInstance(ConfigFactory.getInstance().getCoreSiteConf(), "HdiseaseJob");
		job.setJarByClass(HdiseaseJob.class);
		job.setMapperClass(HdiseaseMap.class);
		job.setReducerClass(HospitalReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("mergeoutput")));
		FileOutputFormat.setOutputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("hdiseaseoutput")));
		return job;
	}
	
	public static void main(String[] args) {
		long starttime = System.currentTimeMillis();
		logger.warn("HdiseaseJob已启动");
		HdiseaseJob hdiseaseJob = new HdiseaseJob();
		try {
			Job job = hdiseaseJob.getJob();
			boolean flag = job.waitForCompletion(true);
			logger.info("HdiseaseJob运行结束，运行结果："+ flag +"，运行耗时"+(System.currentTimeMillis()-starttime));
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			logger.info("HdiseaseJob.getJob", e);
		}
	}
}
