package com.prince.guide.disease;

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

public class DiseaseJob {

	private static Logger logger = LogManager.getLogger("DiseaseJob");
	
	public Job getJob() throws IOException {
		Job job = Job.getInstance(ConfigFactory.getInstance().getCoreSiteConf(), "DiseaseJob");
		job.setJarByClass(DiseaseJob.class);
		job.setMapperClass(DiseaseMap.class);
		job.setReducerClass(HospitalReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("mergeoutput")));
		FileOutputFormat.setOutputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("diseaseoutput")));
		return job;
	}
	
	public static void main(String[] args) {
		long starttime = System.currentTimeMillis();
		logger.warn("DiseaseJob已启动");
		DiseaseJob diseaseJob = new DiseaseJob();
		try {
			Job job = diseaseJob.getJob();
			boolean flag = job.waitForCompletion(true);
			logger.info("DiseaseJob运行结束，运行结果："+ flag +"，运行耗时"+(System.currentTimeMillis()-starttime));
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			logger.info("DiseaseJob.getJob", e);
		}
	}
}
