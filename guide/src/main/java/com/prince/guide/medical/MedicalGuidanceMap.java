package com.prince.guide.medical;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import jodd.util.StringUtil;

public class MedicalGuidanceMap extends Mapper<Object, Text, Text, Text>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String str = value.toString();
		if(StringUtil.isNotEmpty(str)){
			String[] column = str.split("\t");
			//if(column.length==3||column.length==10){
				//把recordid作为key输出
				context.write(new Text(column[0]), new Text(value.toString()));
			//}
		}
	}
}