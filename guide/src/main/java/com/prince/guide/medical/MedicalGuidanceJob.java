package com.prince.guide.medical;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prince.guide.medical.MedicalGuidanceMap;
import com.prince.guide.medical.MedicalGuidanceReduce;
import com.prince.guide.start.ConfigFactory;

public class MedicalGuidanceJob {

	private static Logger logger = LogManager.getLogger("MediclGuidanceJob");
	
	public Job getJob() throws IOException {
		Job job = Job.getInstance(ConfigFactory.getInstance().getCoreSiteConf(), "MedicalGuidanceJob");
		job.setJarByClass(MedicalGuidanceJob.class);
		job.setMapperClass(MedicalGuidanceMap.class);
		job.setReducerClass(MedicalGuidanceReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("todaypath")));
		FileOutputFormat.setOutputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("mergeoutput")));
		return job;
	}
	
	public static void main(String[] args) {
		long starttime = System.currentTimeMillis();
		logger.warn("MedicalGuidanceJob已启动");
		MedicalGuidanceJob medicalGuidanceJob = new MedicalGuidanceJob();
		try {
			Job job = medicalGuidanceJob.getJob();
			boolean flag = job.waitForCompletion(true);
			logger.info("MedicalGuidanceJob运行结束，运行结果："+ flag +"，运行耗时"+(System.currentTimeMillis()-starttime));
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			logger.info("MedicalGuidanceJob.getJob", e);
		}
	}
}
