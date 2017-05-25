package com.prince.guide.start;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.io.Resources;

/**
 * 公共资源配置文件
 * @author princeping
 */
public class ConfigFactory {

	private static ConfigFactory configFactory;//单例
	
	private static Configuration coreSiteConf;//hadoop配置文件
	
	private static Configuration hbaseConf;//hbase配置文件
	
	private static List<String> filepaths;//输入文件路径
	
	private static Map<String, String> hdfspaths;//mr运行路径
	
	private static String resultdata;//解析数据下载存放地址
	
	private static String historydata;//历史数据存放根目录
	
	private static ApplicationContext context;//Spring工具类
	
	private ConfigFactory(){
	}
	
	/**
	 * 获取ConfigFactory实例
	 * @return ConfigFactory实例
	 * @throws DocumentException 
	 */
	public static ConfigFactory getInstance(){
		if(configFactory == null){
			configFactory = new ConfigFactory();
			coreSiteConf = new Configuration();
			coreSiteConf.addResource(Resources.getResource("core-site.xml"));
			hbaseConf = HBaseConfiguration.create();
			hbaseConf.addResource(Resources.getResource("hbase-site.xml"));
			try {
				initfilepaths();
				inithdfspaths();
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			context = new ClassPathXmlApplicationContext("applicationContext.xml");
		}
		return configFactory;
	}
	
	private static void initfilepaths() throws DocumentException{
		SAXReader reader = new SAXReader(); //使用SAXReader方式读取XML文件
		Document doc = reader.read(ConfigFactory.class.getResourceAsStream("/commons.xml"));
		//加载XML配置文件，得到Document对象
		Element root = doc.getRootElement(); //获得根节点
		List<Element> nodes = root.elements("filepaths");
		Element elm = nodes.get(0);
		filepaths = new ArrayList<>();
		filepaths.add(elm.elementText("record"));
		filepaths.add(elm.elementText("reimburse"));
		resultdata = elm.elementText("resultdata");
	}
	
	private static void inithdfspaths() throws DocumentException{
		SAXReader reader = new SAXReader(); //使用SAXReader方式读取XML文件
		Document doc = reader.read(ConfigFactory.class.getResourceAsStream("/commons.xml"));
		//加载XML配置文件，得到Document对象
		Element root = doc.getRootElement(); //获得根节点
		List<Element> nodes = root.elements("hdfspaths");//获得根节点下的子节点
		Element elm = nodes.get(0);
		hdfspaths = new HashMap<>();
		//取得根节点下子节点的内容
		hdfspaths.put("todaypath", elm.elementText("todaypath"));
		hdfspaths.put("mergeoutput", elm.elementText("mergeoutput"));
		hdfspaths.put("hospitaloutput", elm.elementText("hospitaloutput"));
//		hdfspaths.put("diseaseoutput", elm.elementText("diseaseoutput"));
//		hdfspaths.put("hdiseaseoutput", elm.elementText("hdiseaseoutput"));
		hdfspaths.put("resultdata", elm.elementText("resultdata"));
		historydata = elm.elementText("historydata");
		if(historydata.endsWith(File.separator))
			historydata = historydata + File.separator;
	}
	
	/**
	 * 获取hadoop核心配置文件
	 * @return coreSiteConf实例
	 */
	public Configuration getCoreSiteConf(){
		return coreSiteConf;
	}
	
	/**
	 * 获取hbase配置文件
	 * @return hbaseConf实例
	 */
	public Configuration getHbaseConf(){
		return hbaseConf;
	}
	
	/**
	 * 获取输入路径集合
	 * @return 输入路径集合
	 */
	public List<String> getFilepaths(){
		return filepaths;
	}
	
	/**
	 * 获取hdfs路径
	 * @return hdfs操作文件路径
	 */
	public Map<String, String> getHdfspaths(){
		return hdfspaths;
	}
	
	/**
	 * 获取结果集存放目录
	 * @return 结果集存放目录
	 */
	public String getResultData(){
		return resultdata;
	}
	
	/**
	 * 获取历史文件存放目录
	 * @return 历史文件存放目录
	 */
	public String getHistoryData(){
		return historydata;
	}
	
	/**
	 * 获取spring工具类
	 * @return context
	 */
	public ApplicationContext getContext(){
		return context;
	}
}
