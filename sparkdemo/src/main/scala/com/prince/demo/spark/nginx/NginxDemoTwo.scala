package com.prince.demo.spark.nginx

import org.apache.spark.sql.SparkSession

/**
  * 使用映射的方式创建dataframe
  * Created by princeping on 2017/4/24.
  */
object NginxDemoTwo {

  case class CodePhone(date:String, phone:String)

  case class Params(date:String, nickname:String, phone:String, keyword:String, from:String, region:String,
                    insure:String, demand:String, birthday_year:String, birthday_month:String)

  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().master("local[2]").appName("Demo").getOrCreate()
    val spark = SparkSession.builder().master("spark://master1.hadoop:7077").appName("NginxDemoTwo").getOrCreate()

    val input = spark.sparkContext.textFile("hdfs://master1.hadoop:9000/prince/access_20170403.log")

    import spark.implicits._

    val rdd = input
      .filter( line => line.contains("phone=") && line.contains("http://custom.bxd365.com/custom"))
      .map(line => {
        val str = line.substring(line.indexOf("[") + 1, line.indexOf("[") + 21) + "=" +
          line.substring(line.lastIndexOf(" ") + 1, line.length)
        str
      })

    val codeRDD = rdd.filter(line => line.length == 38)
    val paramsRDD = rdd.filter(line => line.length > 100)

    val codeArray = codeRDD.map(line =>{
      val temp = line.split("=")
      CodePhone(temp(0),temp(2))
    })
    val codeDF = codeArray.toDF()
    codeDF.createOrReplaceTempView("code_tmp_table")

    val paramsArray = paramsRDD.map(line =>{
      val str = line.replace("&", "=")
      val temp = str.split("=")
      Params(temp(0), temp(2), temp(4), temp(6), temp(8), temp(10), temp(12), temp(14), temp(16), temp(18))
    })
    val paramsDF =  paramsArray.toDF()
    paramsDF.createOrReplaceTempView("params_tmp_table")

    /*读取数据库提出的电话号*/
    //val dataDF = spark.read.json("E:\\LogA\\nginx\\data\\log\\nginx\\phone.json")
    val dataDF = spark.read.json("hdfs://master1.hadoop:9000/prince/phone.json")
    dataDF.createOrReplaceTempView("data_tmp_table")

    /*取出发送过验证码，但没有传入参数的电话号*/
    val phone1 = spark.sql("select a.date,a.phone from code_tmp_table a left outer join params_tmp_table b on" +
      " a.phone=b.phone where b.phone is null")
    phone1.createOrReplaceTempView("tmp_table1")

    val phone1num = spark.sql("select count(distinct phone) from tmp_table1").take(1).toBuffer(0)(0)

    /*取出日志有记录，但没有写入数据库的信息*/
    val phone2 = spark.sql("select b.date,b.nickname,b.phone,b.keyword,b.from,b.region,b.insure,b.demand," +
      "b.birthday_year,b.birthday_month from params_tmp_table b left outer join data_tmp_table c on" +
      " b.phone=c.phone where c.phone is null")

    phone2.createOrReplaceTempView("tmp_table2")

    val phone2num = spark.sql("select count(distinct phone) from tmp_table2").take(1).toBuffer(0)(0)

//    phone1.repartition(1).write.csv("E:\\new\\code")
//    phone2.repartition(1).write.csv("E:\\new\\params")

    println("发短信未记录的数量：" + phone1num)
    println("未写入库的数量：" + phone2num)

//    val prop = new java.util.Properties
//    prop.setProperty("user", "root")
//    prop.setProperty("password", "123456")
//
//    phone1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.97:3306/xiangju_log", "nginx_code_phone", prop)
//    phone2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.97:3306/xiangju_log", "nginx_params_phone", prop)

    spark.stop()
  }
}
