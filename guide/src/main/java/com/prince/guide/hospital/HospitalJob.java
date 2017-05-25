package com.prince.guide.hospital;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prince.guide.start.ConfigFactory;

public class HospitalJob {

	private static Logger logger = LogManager.getLogger("HospitalJob");
	
	public Job getJob() throws IOException {
		Job job = Job.getInstance(ConfigFactory.getInstance().getCoreSiteConf(), "HospitalJob");
		job.setJarByClass(HospitalJob.class);
		job.setMapperClass(HospitalMap.class);
//		job.setCombinerClass(HospitalCombiner.class);
		job.setReducerClass(HospitalReduce.class);
		job.setPartitionerClass(HospitalPartitioner.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("mergeoutput")));
		FileOutputFormat.setOutputPath(job,
				new Path(ConfigFactory.getInstance().getHdfspaths().get("hospitaloutput")));
		return job;
	}
	
	public static void main(String[] args) {
		long starttime = System.currentTimeMillis();
		logger.warn("HospitalJob已启动");
		HospitalJob hospitalJob = new HospitalJob();
		try {
			Job job = hospitalJob.getJob();
			boolean flag = job.waitForCompletion(true);
			logger.info("HospitalJob运行结束，运行结果："+ flag +"，运行耗时"+(System.currentTimeMillis()-starttime));
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			logger.info("HospitalJob.getJob", e);
		}
	}
}
