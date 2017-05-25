package com.prince.guide.util.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;

/**
 * Hbase工具类接口
 * @author princeping
 */
public interface HbaseUtil {

    /**
     * 判断表是否存在并且是可用状态
     * @param tableName 表名
     * @return 存在返回true，否则返回false
     */
    boolean existsTable(String tableName) throws IOException;

    /**
     * 创建一张表
     * @param tableName 表名
     * @param columnFamilys 列簇集合
     * @throws IOException
     */
    void createTable(String tableName, String columnFamilys[]) throws IOException;

    /**
     * 删除表
     * @param tableName 表名
     * @throws IOException
     */
    void deleteTable(String tableName) throws IOException;

    /**
     * 添加一行数据
     * @param tableName 表名
     * @param row 行号
     * @param columnFamily 列簇
     * @param column 子列（可以空）
     * @param value  具体的值
     * @throws IOException
     */
    void insertRow(String tableName, String row,
                      String columnFamily, String column, String value) throws IOException;

    /**
     * 批量添加数据
     * @param tableName 表名
     * @param rows 行号集合
     * @param columnFamilys 列簇集合
     * @param columns 子列集合 list不能为空，list中的值可以为空
     * @param values 列值集合
     * @throws IOException
     */
    void insertRow(String tableName, List<String> rows,
                      List<String> columnFamilys, List<String> columns, List<String> values) throws IOException;

    /**
     * 批量添加数据
     * @param tableName 表名
     * @param rows 行号集合
     * @param columnFamily 统一列簇
     * @param column 统一子列（可以空）
     * @param values 列值集合
     * @throws IOException
     */
    void insertRow(String tableName, List<String> rows,
                      String columnFamily, String column, List<String> values) throws IOException;

    /**
     * 获取一行数据
     * @param parameter 传入参数顺序：表名，行健，列簇（可以空），子列（可以空）
     * @return 查询到的数据
     * @throws IOException
     */
    List<Cell> getDataForGet(String... parameter) throws IOException;

    /**
     * 根据row，批量获取多行记录
     * @param tableName 表名
     * @param rows 行健
     * @return 查询到的数据
     * @throws IOException
     */
    List<List<Cell>> getDataForGet(String tableName, List<String> rows) throws IOException;

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
    List<List<Cell>> getSequenceData(String tableName, String startrow, String endrow, String columnFamily, String column) throws IOException;

    /**
     * 分页获取数据
     * @param tableName 表名
     * @param startrow 开始行键(可以为空，为空时从头取数据，不包含startrow本身)
     * @param size 分页尺寸
     * @return 取得的数据
     */
    List<List<Cell>> getPageData(String tableName, String startrow, int size) throws IOException;
    
    /**
     * 删除一条数据
     * @param tableName 表名
     * @param row 行健
     * @throws IOException
     */
    void delRow(String tableName, String row) throws IOException;

    /**
     * 批量删除数据
     * @param tableName 表名
     * @param rows 行健集合
     * @throws IOException
     */
    void delRow(String tableName, List<String> rows) throws IOException;

}
