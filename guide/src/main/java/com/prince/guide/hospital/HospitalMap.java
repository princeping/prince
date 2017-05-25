package com.prince.guide.hospital;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import jodd.util.StringUtil;

public class HospitalMap extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String str = value.toString();
		if(StringUtil.isNotEmpty(str)){
			String[] message = str.split("\t");
			if(message.length==12){
				//0住院总人次 1住院总花费 2住院总报销 3住院总天数 4住院总治愈 5门诊总人次 6门诊总花费 7门诊总报销 8门诊总住院 9门诊总治愈
				double[] coll = new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
				int line = 0;
				if(!"1".equals(message[5]))
					line = 5;
				coll[0+line] = 1;
				coll[1+line] = Double.valueOf(message[8]);
				coll[2+line] = Double.valueOf(message[11]);
				long day;
				try {
					day = getDay(message[6], message[7]);
				} catch (ParseException e) {
					return;
				}
				coll[3+line] = day;
				if("1".equals(message[9]))
					coll[4+line] = 1;
				context.write(new Text("hos"+message[1]),
						new Text(coll[0] + "\t" + coll[1] + "\t" + coll[2] + "\t" + coll[3] + "\t" + coll[4] + "\t"
								+ coll[5] + "\t" + coll[6] + "\t" + coll[7] + "\t" + coll[8] + "\t" + coll[9]));
				context.write(new Text("dis"+message[2]),
						new Text(coll[0] + "\t" + coll[1] + "\t" + coll[2] + "\t" + coll[3] + "\t" + coll[4] + "\t"
								+ coll[5] + "\t" + coll[6] + "\t" + coll[7] + "\t" + coll[8] + "\t" + coll[9]));
				context.write(new Text("hdis"+message[1]+"^^"+message[2]),
						new Text(coll[0] + "\t" + coll[1] + "\t" + coll[2] + "\t" + coll[3] + "\t" + coll[4] + "\t"
								+ coll[5] + "\t" + coll[6] + "\t" + coll[7] + "\t" + coll[8] + "\t" + coll[9]));
			}
		}
	}
	
	/**
	 * 计算住院天数
	 * @param startday 起始日期
	 * @param stopday 出院日期
	 * @return 住院天数
	 * @throws ParseException
	 */
	private long getDay(String startday, String stopday) throws ParseException{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date date1 = format.parse(startday);
		Date date2 = format.parse(stopday);
		return (date2.getTime()-date1.getTime())/(1000*60*60*24);
	}
}
