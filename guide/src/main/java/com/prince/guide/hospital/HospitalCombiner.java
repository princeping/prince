package com.prince.guide.hospital;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import jodd.util.StringUtil;

public class HospitalCombiner extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//0住院总人次 1住院总花费 2住院总报销 3住院总天数 4住院总治愈 5门诊总人次 6门诊总花费 7门诊总报销 8门诊总住院 9门诊总治愈
		double[] coll = new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		for(Text text : values){
			String tempStr = text.toString();
			String[] tempArray = StringUtil.split(tempStr, "\t");
			for(int cnt=0; cnt<10; cnt++){
				coll[cnt] = coll[cnt] + Double.valueOf(tempArray[cnt]);
			}
		}
		context.write(key, new Text(coll[0] + "\t" + coll[1] + "\t" + coll[2] + "\t" + coll[3] + "\t" + coll[4] + "\t"
								+ coll[5] + "\t" + coll[6] + "\t" + coll[7] + "\t" + coll[8] + "\t" + coll[9]));
	}
}