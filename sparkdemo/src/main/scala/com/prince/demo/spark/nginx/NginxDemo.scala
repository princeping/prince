package com.prince.demo.spark.nginx

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用组装json格式的方式获得dataframe
  * Created by princeping on 2017/4/21.
  */
object NginxDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("NginxDemo").getOrCreate()
    val input = spark.sparkContext.textFile("E:\\LogA\\nginx\\data\\log\\nginx\\access_20170401.log")

    val rdd = input
      .filter( line => line.contains("phone=") && line.contains("http://custom.bxd365.com/custom"))
      .map(line => {
        val str = line.substring(line.indexOf("[")+1,line.indexOf("[")+21) + "\t" +
          line.substring(line.lastIndexOf(" ")+1,line.length)
        val json = str
          .replace(str, "{\"date\":\"" + str + "\"}")
          .replace("&", "\",\"")
          .replace("=", "\":\"")
          .replace("\t", "\",\"")
        json
      })

    /*过滤出短信验证码信息*/
    val codeJson = rdd.filter(line => line.length == 53)
    val paramsJson = rdd.filter(line => line.length > 100)
    //短信信息实际长度为53

    val codeDF = spark.read.json(codeJson)
    codeDF.createOrReplaceTempView("code_tmp_table")

    val paramsDF = spark.read.json(paramsJson)
    paramsDF.createOrReplaceTempView("params_tmp_table")

    /*读取数据库提出的电话号*/
    val dataDF = spark.read.json("E:\\LogA\\nginx\\data\\log\\nginx\\phone.json")
    dataDF.createOrReplaceTempView("data_tmp_table")

    /*取出发送过验证码，但没有传入参数的电话号*/
    val phone1 = spark.sql("select a.date,a.phone from code_tmp_table a left outer join params_tmp_table b on" +
      " a.phone=b.phone where b.phone is null")
    phone1.createOrReplaceTempView("tmp_table1")

    //val ph1 = phone1.dropDuplicates(Seq("phone"))

    //val ph1 = spark.sql("select date,phone from (select *,row_number() over (partition by phone order by date) num from tmp_table1) t where t.num=1")

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

//    ph1.repartition(1).write.csv("E:\\111" )

    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")

    phone1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.97:3306/xiangju_log", "nginx_code_phone", prop)
    phone2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.97:3306/xiangju_log", "nginx_params_phone", prop)

    spark.stop()
  }
}
