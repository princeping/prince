package com.prince.guide.disease;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import jodd.util.StringUtil;

public class DiseaseMap extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String str = value.toString();
		if(StringUtil.isNotEmpty(str)){
			String[] message = str.split("\t");
			if(message.length==12){
				context.write(new Text("dis" + message[2]), new Text(str));
			}
		}
	}
}
