package com.prince.guide.hospital;

import java.io.IOException;
import java.text.NumberFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.prince.guide.start.ConfigFactory;
import com.prince.guide.util.redis.RedisUtil;

import jodd.util.StringUtil;

public class HospitalReduce extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//0住院总人次 1住院总花费 2住院总报销 3住院总天数 4住院总治愈 5门诊总人次 6门诊总花费 7门诊总报销 8门诊总住院 9门诊总治愈
		double[] coll = new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		countNumber(coll, values);
		RedisUtil redisUtil = (RedisUtil)ConfigFactory.getInstance().getContext().getBean("redisUtil");
		String rowkey = key.toString();
		mergeNumber(redisUtil, rowkey, coll);
		StringBuffer sb = new StringBuffer();
		for(double temp : coll)
			sb.append(temp+"\t");
		redisUtil.addString(rowkey, sb.substring(0, sb.length()-1));
//		NumberFormat nf = NumberFormat.getIntegerInstance();
//      nf.setMaximumFractionDigits(2);
//      nf.setGroupingUsed(false);
		writeResult(key, context, coll);
	}
	
	/**
	 * 合并历史数据
	 * @param redisUtil redis工具类
	 * @param rowkey 行键
	 * @param coll 统计数据
	 * @throws IOException
	 */
	private void mergeNumber(RedisUtil redisUtil, String rowkey, double[] coll) throws IOException{
		String stringCell = redisUtil.getString(rowkey);
		if(StringUtil.isNotEmpty(stringCell)){
			String[] arrayCell = StringUtil.split(stringCell, "\t");
			if(arrayCell!=null&&arrayCell.length==10){
				for(int cnt=0; cnt<10; cnt++){
					coll[cnt] = coll[cnt] + Double.valueOf(arrayCell[cnt]);
				}
			}
		}
	}
	
	/**
	 * 统计某日数据
	 * @param coll 数据结果数组
	 * @param values 当天原始数据
	 */
	private void countNumber(double[] coll, Iterable<Text> values){
		for(Text text : values){
			String tempStr = text.toString();
			String[] tempArray = StringUtil.split(tempStr, "\t");
			for(int cnt=0; cnt<10; cnt++){
				coll[cnt] = coll[cnt] + Double.valueOf(tempArray[cnt]);
			}
		}
	}
	
    /**
     * 输出运算结果
     * @param key 输入的key
     * @param context context对象
     * @param coll 结果集合
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeResult(Text key, Context context, double[] coll) throws IOException, InterruptedException{
        NumberFormat nf = NumberFormat.getIntegerInstance();
        nf.setMaximumFractionDigits(2);
        nf.setGroupingUsed(false);
        String hstr,ostr;
        if(coll[0]>0){
            hstr = coll[0]+"\t"+nf.format(coll[1]/coll[0])+"\t"+nf.format(coll[2]/coll[0])
                    +"\t"+nf.format((coll[2]/coll[1])*100)+"\t"+nf.format(coll[3]/coll[0]) +"\t"+nf.format(coll[4]/coll[0]);
        }else{
            hstr = "0\t0\t0\t0\t0\t0";
        }
        if(coll[5]>0){
            ostr = "\t"+coll[5]+"\t"+nf.format(coll[6]/coll[5])+"\t"+nf.format(coll[7]/coll[5])
                    +"\t"+nf.format((coll[7]/coll[6])*100)+"\t"+nf.format(coll[9]/coll[5]);
        }else{
            ostr = "\t0\t0\t0\t0\t0";
        }
        context.write(key, new Text(hstr+ostr));
    }
}
