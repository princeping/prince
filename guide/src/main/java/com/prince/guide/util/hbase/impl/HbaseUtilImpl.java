package com.prince.guide.util.hbase.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.prince.guide.start.ConfigFactory;
import com.prince.guide.util.hbase.HbaseUtil;

import jodd.util.StringUtil;

/**
 * Hbase工具实现类
 * @author princeping
 */
public class HbaseUtilImpl implements HbaseUtil{
	
    /**
     * 判断表是否存在并且是可用状态
     * @param tableName 表名
     * @return 存在返回true，否则返回false
     */
	@Override
	public boolean existsTable(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Admin admin = connection.getAdmin();
		boolean flag = admin.tableExists(TableName.valueOf(tableName));
		if(flag)
			flag = admin.isTableEnabled(TableName.valueOf(tableName));
		admin.close();
		connection.close();
		return flag;
	}

    /**
     * 创建一张表
     * @param tableName 表名
     * @param columnFamilys 列簇集合
     * @throws IOException
     */
	@Override
	public void createTable(String tableName, String[] columnFamilys) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Admin admin = connection.getAdmin();
		TableName tableName1 = TableName.valueOf(tableName);
		if(admin.tableExists(tableName1))
			return;
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
		for(String colfamily : columnFamilys){
			hTableDescriptor.addFamily(new HColumnDescriptor(colfamily));
		}
		admin.createTable(hTableDescriptor);
		admin.close();
		connection.close();
	}

    /**
     * 删除表
     * @param tableName 表名
     * @throws IOException
     */
	@Override
	public void deleteTable(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Admin admin = connection.getAdmin();
		TableName tableName1 = TableName.valueOf(tableName);
		if(!admin.tableExists(tableName1))
			return;
		admin.disableTable(tableName1);
		admin.deleteTable(tableName1);
		admin.close();
		connection.close();
	}

    /**
     * 添加一行数据
     * @param tableName 表名
     * @param row 行号
     * @param columnFamily 列簇
     * @param column 子列（可以空）
     * @param value  具体的值
     * @throws IOException
     */
	@Override
	public void insertRow(String tableName, String row, String columnFamily, String column, String value)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(row));
		if(StringUtil.isEmpty(column))
			put.addColumn(Bytes.toBytes(columnFamily), null, Bytes.toBytes(value));
		else
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		table.close();
		connection.close();
	}

    /**
     * 批量添加数据
     * @param tableName 表名
     * @param rows 行号集合
     * @param columnFamilys 列簇集合
     * @param columns 子列集合 list不能为空，list中的值可以为空
     * @param values 列值集合
     * @throws IOException
     */
	@Override
	public void insertRow(String tableName, List<String> rows, List<String> columnFamilys, List<String> columns,
			List<String> values) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
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
		table.close();
		connection.close();
	}

    /**
     * 批量添加数据
     * @param tableName 表名
     * @param rows 行号集合
     * @param columnFamily 统一列簇
     * @param column 统一子列（可以空）
     * @param values 列值集合
     * @throws IOException
     */
	@Override
	public void insertRow(String tableName, List<String> rows, String columnFamily, String column, List<String> values)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		List<Put> listput = new ArrayList<>(rows.size());
		byte[] columnbyte = null;
		if(StringUtil.isNotEmpty(column))
            columnbyte = Bytes.toBytes(column);
        byte[] family = Bytes.toBytes(columnFamily);
        for(int cnt=0;cnt<rows.size();cnt++){
            Put put = new Put(Bytes.toBytes(rows.get(cnt)));
            put.addColumn(family,columnbyte,Bytes.toBytes(values.get(cnt)));
            listput.add(put);
		}
		table.put(listput);
		table.close();
		connection.close();
	}

    /**
     * 获取一行数据
     * @param parameter 传入参数顺序：表名，行健，列簇（可以空），子列（可以空）
     * @return 查询到的数据
     * @throws IOException
     */
	@Override
	public List<Cell> getDataForGet(String... parameter) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(parameter[0]));
		Get get = new Get(Bytes.toBytes(parameter[1]));
		if(parameter.length==3){
			get.addFamily(Bytes.toBytes(parameter[2]));
		}else if(parameter.length==4){
			get.addColumn(Bytes.toBytes(parameter[2]), Bytes.toBytes(parameter[3]));
		}		
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		table.close();
		connection.close();
		return cells;
	}

    /**
     * 根据row，批量获取多行记录
     * @param tableName 表名
     * @param rows 行健
     * @return 查询到的数据
     * @throws IOException
     */
	@Override
	public List<List<Cell>> getDataForGet(String tableName, List<String> rows) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		 List<Get> listget = new ArrayList<>(rows.size());
	        for(String row : rows){
	            Get get = new Get(Bytes.toBytes(row));
	            listget.add(get);
	        }
	        Result[] results = table.get(listget);
	        List<List<Cell>> returnValue = null;
	        if(results!=null&&results.length>0){
	            returnValue = new ArrayList<>(results.length);
	            for(Result result : results){
	                returnValue.add(result.listCells());
	            }
	        }
	        return returnValue;
	}

    /**
     * 根据开始的rowkey和结束的rowkey，顺序获取中间所有的行
     * @param tableName 表名
     * @param startrow 开始行健
     * @param endrow 结束行健
     * @param columnFamily 列簇（可以空）
     * @param column 子列（可以空）
     * @return 查询到的数据
     * @throws IOException
     */
	@Override
	public List<List<Cell>> getSequenceData(String tableName, String startrow, String endrow, String columnFamily,
			String column) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		 Scan scan = new Scan();
	        if(StringUtil.isNotEmpty(startrow))
	            scan.setStartRow(Bytes.toBytes(startrow));
	        byte[] endbytes = Bytes.toBytes("endrow");
	        endbytes[endbytes.length-1]++;
	        scan.setStopRow(endbytes);
	        if(StringUtil.isNotEmpty(columnFamily)&&StringUtil.isNotEmpty(column)){
	            scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
	        }else if(StringUtil.isNotEmpty(columnFamily)){
	            scan.addFamily(Bytes.toBytes(columnFamily));
	        }
	        ResultScanner results = table.getScanner(scan);
	        List<List<Cell>> returnValue = null;
	        if(results!=null){
	            returnValue = new ArrayList<>();
	            for(Result result : results){
	                returnValue.add(result.listCells());
	            }
	        }
	        return returnValue;
	}
	
    /**
     * 分页获取数据
     * @param tableName 表名
     * @param startrow 开始行键(可以为空)
     * @param size 分页尺寸
     * @return 取得的数据
     */
	@Override
	public List<List<Cell>> getPageData(String tableName, String startrow, int size) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		if(StringUtil.isNotEmpty(startrow)){
			byte[] startbyte = Bytes.toBytes(startrow);
			startbyte[startbyte.length-1]++;
			scan.setStartRow(Bytes.toBytes(startrow));
		}
		Filter filter = new PageFilter(size);
		scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        List<List<Cell>> returnValue = null;
        if(results!=null){
            returnValue = new ArrayList<>();
            for(Result result : results){
                returnValue.add(result.listCells());
            }
        }
        return returnValue;
	}

    /**
     * 删除一条数据
     * @param tableName 表名
     * @param row 行健
     * @throws IOException
     */
	@Override
	public void delRow(String tableName, String row) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(row));
		table.delete(delete);
		table.close();
		connection.close();
	}

    /**
     * 批量删除数据
     * @param tableName 表名
     * @param rows 行健集合
     * @throws IOException
     */
	@Override
	public void delRow(String tableName, List<String> rows) throws IOException {
		Connection connection = ConnectionFactory.createConnection(
				ConfigFactory.getInstance().getHbaseConf());
		Table table = connection.getTable(TableName.valueOf(tableName));
		List<Delete> deletelist = new ArrayList<>(rows.size());
		for(String row : rows){
			Delete delete = new Delete(Bytes.toBytes(row));
			deletelist.add(delete);
		}
		table.delete(deletelist);
		table.close();
		connection.close();
	}

}
