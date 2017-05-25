package com.prince.guide.hospital;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HospitalPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String keyStr = key.toString();
		if(keyStr.indexOf("hos")>-1)
			return 0;
		else if(keyStr.indexOf("hdis")>-1)
			return 1;
		else
			return 2;
	}
}
