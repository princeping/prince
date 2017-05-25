package com.prince.guide.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.prince.guide.BaseJunit4Test;
import com.prince.guide.util.hbase.HbaseUtil;
import com.prince.guide.util.hbase.impl.HbaseUtilImpl;

public class HbaseUtilTest extends BaseJunit4Test{

	private HbaseUtil hbaseUtil = new HbaseUtilImpl();
	
	private Logger logger = LogManager.getLogger("HbaseUtilTest");
	
	@Test
	public void existsTableTest(){
		try {
			System.out.println(hbaseUtil.existsTable("table1"));
		} catch (IOException e) {
			logger.error("HbaseUtilTest.existsTableTest", e);
		}
	}
	
	@Test
	public void createTableTest(){
		try {
		String[] columnFamilys = {"hospital", "disease", "hdisease", "default"};
			hbaseUtil.createTable("guide", columnFamilys);
		} catch (IOException e) {
			logger.error("HbaseUtilTest.createTableTest", e);
		}
	}
	
	@Test
	public void deleteTableTest(){
		try {
			hbaseUtil.deleteTable("guide");
		} catch (IOException e) {
			logger.error("HbaseUtilTest.deleteTableTest", e);
		}
	}
	
	@Test
	public void insertRow1Test(){
		try {
			hbaseUtil.insertRow("table1", "1", "columnFamily1", "column", "value1");
		} catch (IOException e) {
			logger.error("HbaseUtilTest.insertRowTest1", e);
		}
	}
	
	@Test
	public void insertRow2Test(){
		try {
		List<String> rows = new ArrayList<String>();
		rows.add("0");
		rows.add("1");
		rows.add("2");
		List<String> columns = new ArrayList<String>();
		columns.add("column1");
		columns.add("column2");
		columns.add("column3");
		List<String> values = new ArrayList<String>();
		values.add("values1");
		values.add("values2");
		values.add("values3");
		List<String> columnFamilys = new ArrayList<String>();
		columnFamilys.add("columnFamily1");
		columnFamilys.add("columnFamily2");
		columnFamilys.add("columnFamily3");
			hbaseUtil.insertRow("tablename2", rows, columnFamilys, columns, values);
		} catch (IOException e) {
			logger.error("HbaseUtilTest.insertRowTest2", e);
		}
	}
	
	@Test
	public void insertRowTest3(){
		try {
		List<String> rows = new ArrayList<String>();
		rows.add("0");
		rows.add("1");
		rows.add("2");
		List<String> values = new ArrayList<String>();
		values.add("values1");
		values.add("values2");
		values.add("values3");
			hbaseUtil.insertRow("tablename3", rows, "columnFamily1", null, values);
		} catch (IOException e) {
			logger.error("HbaseUtilTest.insertRowTest3", e);
		}
	}
	
	@Test
	public void getDataForGet1Test(){
		try {
			System.out.println(hbaseUtil.getDataForGet("tablename1, 0"));
		} catch (IOException e) {
			logger.error("HbaseUtilTest.getDataForGet1Test", e);
		}
	}
	
	@Test
	public void getDataForGet2Test(){
		List<String> rows = new ArrayList<String>();
		rows.add("0");
		rows.add("1");
		rows.add("2");
		rows.add("3");
		rows.add("4");
		rows.add("5");
		rows.add("6");
		rows.add("7");
		try {
			System.out.println(hbaseUtil.getDataForGet("guide", rows));
		} catch (IOException e) {
			logger.error("HbaseUtilTest.getDataForGet2Test", e);
		}
	}
	
	@Test
	public void getSequenceDataTest(){
		try {
			hbaseUtil.getSequenceData("tablename1", "1", "2", "columnFamily1", "");
		} catch (IOException e) {
			logger.error("HbaseUtilTest.getSequenceDataTest", e);
		}
	}
	
	@Test
	public void delRow1Test(){
		try {
			hbaseUtil.delRow("tableName2", "0");
		} catch (IOException e) {
			logger.error("HbaseUtilTest.deRow1Test", e);
		}
	}
	
	@Test
	public void delRow2Test(){
		try {
		List<String> rows = new ArrayList<String>();
		rows.add("1");
		rows.add("2");
			hbaseUtil.delRow("tableName2", rows);
		} catch (IOException e) {
			logger.error("HbaseUtilTest.deRow2Test", e);
		}
	}
}
