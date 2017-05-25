package com.prince.demo.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/3/17.
  */
object SparkToMysql {

  case class json(name:String, age:Int, city:String, sex:String)

  def toMySQL(iterable: Iterable[(String, Int, String, String)]): Unit ={
    var conn:Connection = null
    var ps:PreparedStatement = null
    val sql = "insert into info(name, age, city, sex) values (?, ?, ?, ?)"
    try{
      Class.forName("com,mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
      iterable.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setString(3, line._3)
        ps.setString(4, line._4)
        ps.executeUpdate()
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
  }

//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder().master("local").appName("JsonDemo").getOrCreate()
//    val path = "E:\\日志分析\\json.json"
//    val peopleDF = session.read.json(path)
//    peopleDF.createOrReplaceTempView("student")
//    val teenagerNamesDF = session.sql("SELECT name,age,city,sex From student")
//    val teenRDD = teenagerNamesDF.rdd
//    teenRDD.foreachPartition(toMySQL)
//  }
}
