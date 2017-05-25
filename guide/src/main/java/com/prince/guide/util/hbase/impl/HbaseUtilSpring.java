package com.prince.guide.util.hbase.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.ResultsExtractor;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.prince.guide.start.ConfigFactory;
import com.prince.guide.util.hbase.HbaseUtil;

import jodd.util.StringUtil;

/**
 * Hbase工厂类spring实现
 * @author princeping
 */
public class HbaseUtilSpring implements HbaseUtil{
	
	private HbaseTemplate hbaseTemplate =  
			(HbaseTemplate)ConfigFactory.getInstance().getContext().getBean("htemplate");
	
	private HbaseUtil hbaseUtil = new HbaseUtilImpl();

	@Override
	public boolean existsTable(String tableName) throws IOException {
		return hbaseUtil.existsTable(tableName);
	}

	@Override
	public void createTable(String tableName, String[] columnFamilys) throws IOException {
		hbaseUtil.createTable(tableName, columnFamilys);
	}

	@Override
	public void deleteTable(String tableName) throws IOException {
		hbaseUtil.deleteTable(tableName);
	}

	@Override
	public void insertRow(String tableName, String row, String columnFamily, String column, String value)
			throws IOException {
		if(StringUtil.isNotEmpty(column)){
			hbaseTemplate.put(tableName, row, columnFamily, column, Bytes.toBytes(value));
		}else{
			List<String> rows = new ArrayList<>(1);
			rows.add(row);
			List<String> columnFamilys = new ArrayList<>(1);
			columnFamilys.add(columnFamily);
			List<String> columns = new ArrayList<>(1);
			columns.add(column);
			List<String> values = new ArrayList<>(1);
			values.add(value);
			hbaseTemplate.execute(tableName, new InsertRowCallback(rows, columnFamilys, columns, values));
		}
	}
	
	private class InsertRowCallback implements TableCallback<Boolean>{

		private List<String> rows,columnFamilys,columns,values;
		
		public InsertRowCallback(List<String> rows, List<String> columnFamilys, List<String> columns, List<String> values){
			this.rows = rows;
			this.columnFamilys = columnFamilys;
			this.columns = columns;
			this.values = values;
		}
		@Override
		public Boolean doInTable(HTableInterface table) throws Throwable {
			List<Put> listput = new ArrayList<>(rows.size());
			for(int cnt=0; cnt<rows.size(); cnt++){
				Put put = new Put(Bytes.toBytes(rows.get(cnt)));
				byte[] column = null;
				if(StringUtil.isNotEmpty(columns.get(cnt)))
					column = Bytes.toBytes(columns.get(cnt));
				put.addColumn(Bytes.toBytes(columnFamilys.get(cnt)), column, Bytes.toBytes(values.get(cnt)));
				listput.add(put);
			}
			table.put(listput);
			return true;
		}
		
	}

	@Override
	public void insertRow(String tableName, List<String> rows, List<String> columnFamilys, List<String> columns,
			List<String> values) throws IOException {
		hbaseTemplate.execute(tableName, new InsertRowCallback(rows, columnFamilys, columns, values));
	}

	@Override
	public void insertRow(String tableName, List<String> rows, String columnFamily, String column, List<String> values)
			throws IOException {
		List<String> columnFamilys = new ArrayList<>(rows.size());
		List<String> columns = new ArrayList<>(rows.size());
		for(int cnt=0; cnt<rows.size(); cnt++){
			columnFamilys.add(columnFamily);
			columns.add(column);
		}
		hbaseTemplate.execute(tableName, new InsertRowCallback(rows, columnFamilys, columns, values));
	}

	@Override
	public List<Cell> getDataForGet(String... parameter) throws IOException {
		if(parameter.length == 2)
			return hbaseTemplate.get(parameter[0], parameter[1], new RowMapperOne());
		else if(parameter.length == 3)
			return hbaseTemplate.get(parameter[0], parameter[1], parameter[2], new RowMapperOne());
		else if(parameter.length == 4)
			return hbaseTemplate.get(parameter[0], parameter[1], parameter[2], parameter[2], new RowMapperOne());
		else
			return null;
	}
	
	private class RowMapperOne implements RowMapper<List<Cell>>{

		@Override
		public List<Cell> mapRow(Result result, int i) throws Exception {
			return result.listCells();
		}
		
	}

	@Override
	public List<List<Cell>> getDataForGet(String tableName, List<String> rows) throws IOException {
		return hbaseUtil.getDataForGet(tableName, rows);
	}

	@Override
	public List<List<Cell>> getSequenceData(String tableName, String startrow, String endrow, String columnFamily,
			String column) throws IOException {
		Scan scan = new Scan();
		if(StringUtil.isNotEmpty(startrow))
			scan.setStartRow(Bytes.toBytes(startrow));
		byte[] endbytes = Bytes.toBytes(endrow);
		endbytes[endbytes.length-1]++;
		scan.setStopRow(endbytes);
		if(StringUtil.isNotEmpty(columnFamily)&&StringUtil.isNotEmpty(column)){
			scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		}else if(StringUtil.isNotEmpty(columnFamily)){
			scan.addFamily(Bytes.toBytes(columnFamily));
		}
		return hbaseTemplate.find(tableName, scan, new RowMappers());
	}
	
	private class RowMappers implements ResultsExtractor<List<List<Cell>>>{

		@Override
		public List<List<Cell>> extractData(ResultScanner resultScanner) throws Exception {
			List<List<Cell>> returnValue = null;
			if(resultScanner!=null){
				returnValue = new ArrayList<>();
				for(Result result : resultScanner){
					returnValue.add(result.listCells());
				}
			}
			return returnValue;
		}
		
	}

	@Override
	public void delRow(String tableName, String row) throws IOException {
		List rows = new ArrayList<>(1);
		rows.add(row);
		hbaseTemplate.execute(tableName, new DeleteCallback(rows));
	}

	@Override
	public void delRow(String tableName, List<String> rows) throws IOException {
		hbaseTemplate.execute(tableName, new DeleteCallback(rows));
	}	
	private class DeleteCallback implements TableCallback<Boolean>{

		private List<String> rows;
			
		public DeleteCallback(List<String> rows) {
			this.rows = rows;
		}
		
		@Override
		public Boolean doInTable(HTableInterface table) throws Throwable {
			List<Delete> listdelete = new ArrayList<>(rows.size());
			for(String rowkey : rows){
				Delete delete = new Delete(Bytes.toBytes(rowkey));
				listdelete.add(delete);
			}
			table.delete(listdelete);
			return true;
		}
	}

	@Override
	public List<List<Cell>> getPageData(String tableName, String startrow, int size) throws IOException {
		return hbaseUtil.getPageData(tableName, startrow, size);
	}
}
