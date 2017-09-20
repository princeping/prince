package com.prince.demo.hbase

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by princeping on 2017/8/9.
  */
object HBaseClient {

  private val config = ConfigFactory.load()
  private val conn = getConnection

  /**
    * 扫描HBase并返回结果
    * @param tableName 表明
    * @param filter 过滤条件
    * @param startRow 起始行键
    * @param stopRow 结束行键
    * @return 扫描结果
    */
  def scan(tableName: String, filter: Filter, startRow: String, stopRow: String): List[Map[String, String]] = {
    val s = buildScan(filter, startRow, stopRow)
    val t = conn.getTable(TableName.valueOf(tableName))
    scan(t, s)
  }

  /**
    * 执行扫描
    * @param table 表
    * @param scan scan
    * @return
    */
  private def scan(table: Table, scan: Scan): List[Map[String, String]] = {
    val scanner = table.getScanner(scan)
    val ite = scanner.iterator()
    val result = new ListBuffer[Map[String, String]]
    while (ite.hasNext){
      val map = new mutable.ListMap[String, String]
      ite.next().listCells().foreach(c => map += readCell(c))
      result += map.toMap
    }
    result.toList
  }

  /**
    * 读取单元格
    * @param cell 单元格
    * @return
    */
  private def readCell(cell: Cell) = {
    val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
    val value = Bytes.toString(CellUtil.cloneValue(cell))
    (qualifier, value)
  }

  /**
    * 构建Scan实例
    * @param filter 过滤条件
    * @param startRow 起始行键
    * @param stopRow 结束行键
    * @return
    */
  private def buildScan(filter: Filter, startRow: String, stopRow: String): Scan ={
    val scan = new Scan()
    scan.setMaxVersions()
    scan.setCaching(2000)
    scan.setCacheBlocks(false)
    if(filter != null)
      scan.setFilter(filter)
    if(startRow != null)
      scan.setStartRow(Bytes.toBytes(startRow))
    if(stopRow != null)
      scan.setStopRow(Bytes.toBytes(stopRow))
    scan
  }

  /**
    * 获取链接
    */
  private def getConnection: Connection = {
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, config.getString("hbase.quorum"))
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, config.getString("hbase.port"))
    ConnectionFactory.createConnection(conf)
  }
}
