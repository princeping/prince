package com.prince.guide.medical;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class MedicalGuidanceReduce extends Reducer<Text, Text, NullWritable, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		double recost = 0;
		String datalist = null;
		String reimbursetime = null;
		//遍历Map传入的values值
		for(Text text : values){
			//将遍历出的text转化为string类型
			String str = text.toString();
			//分割并组装str为string数组
			String[] message = str.split("\t");
			if(message.length==3){
				recost = recost + Double.valueOf(message[2]);
				reimbursetime = message[1];
			}else{
				datalist = text.toString();
			}
		}
		context.write(NullWritable.get(), new Text(datalist + "\t" + reimbursetime + "\t" + recost));
	}
}