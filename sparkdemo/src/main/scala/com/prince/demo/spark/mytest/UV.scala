package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * 计算UV
  * Created by princeping on 2017/5/16.
  */
object UV {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DemoThree").getOrCreate()
    val sc = spark.sparkContext

    val input = sc.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\0501.log")

    val restRDD = input
      .map(line => (line.split("\\|")(0).split(",")(0),line.split("\\|")(3),line.split("\\|")(6),line.split("\\|")(10)))
      .filter(line => line._2.contains("rest-01.bxd365.com"))

    val rdd1 = restRDD
      .filter(line => line._3.contains("/getWelcomeImages"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd2 = restRDD
      .filter(line => line._3.contains("/login"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd3 = restRDD
      .filter(line => line._3.contains("/V6_getIndexad") | line._3.contains("/api/showPlanbook/check")
        | line._3.contains("/activity_log") | line._3.contains("/api/listenDaily/itemlist")
        | line._3.contains("/api/indexs/navigation") | line._3.contains("/bxdInsurProduct/lists"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd4 = restRDD
      .filter(line => line._3.contains("/api/elite/lists") | line._3.contains("/api/weiyuedu/lists")
        | line._3.contains("/api/clause") | line._3.contains("/api/weiyuedu"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd5 = restRDD
      .filter(line => line._3.contains("/html/singlePage/diamondInfo") | line._3.contains("/html/baoshiApi/")
        | line._3.contains("/api/diamond/serveplatefrom") | line._3.contains("/html/Product/baoshi/index"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd6 = restRDD
      .filter(line => line._3.contains("/v6_getConch") | line._3.contains("/html/weizhanpage/card?")
        | line._3.contains("/api/Weizhan/microCard") | line._3.contains("/wallet")
        | line._3.contains("/api/Conch/mylog") | line._3.contains("/conchshop/state")
        | line._3.contains("/getInsureTags") | line._3.contains("/addInsureTags")
        | line._3.contains("/rest.php?r=rhelper/Certificate") | line._3.contains("/company")
        | line._3.contains("/modify_my_information") | line._3.contains("/upload_avatar")
        | line._3.contains("/realname_certification") | line._3.contains("/MyMrfavq")
        | line._3.contains("/api/collect/my") | line._3.contains("/my_plans")
        | line._3.contains("/v6_feedback") | line._3.contains("/v6_myfeedback")
        | line._3.contains("/api/my/modifySettings") | line._3.contains("/change_user_identity"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd7 = restRDD
      .filter(line => line._3.contains("/api/Weizhan") | line._3.contains("/getMyProducts")
        | line._3.contains("/weizhan"))
      .map(line => (line._1, line._4)).distinct().count


    val rdd8 = restRDD
      .filter(line => line._3.contains("/api/commonIntface/adlist") | line._3.contains("/api/baoxianVideo/clist")
        | line._3.contains("/api/baoxianVideo/itemlist") | line._3.contains("/html/baoxianVideo")
        | line._3.contains("/api/baoxianVideo/agent") | line._3.contains("/api/commonIntface/historyAdd")
        | line._3.contains("/api/comment/publiclist") | line._3.contains("/api/baoxianVideo/exchange")
        | line._3.contains("/api/commonIntface/historyDel"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd9 = restRDD
      .filter(line => line._3.contains("/api/wenda/signNum") | line._3.contains("/rest.php?r=rww/list")
        | line._3.contains("/rest.php?r=rww/catenum") | line._3.contains("/v6_getTaskLis")
        | line._3.contains("/getReward") | line._3.contains("/api/wenda/initiateChat")
        | line._3.contains("/rest.php?r=rww/keysearch") | line._3.contains("/api/commonIntface/multiUserInfo")
        | line._3.contains("/rest.php?r=rww/doneQuestion") | line._3.contains("/rest.php?r=rww/myfavq")
        | line._3.contains("/rest.php?r=rww/getanswers") | line._3.contains("/api/wenda/initiateGrade")
        | line._3.contains("/api/wenda/rankInfo") | line._3.contains("/api/wenda/rankList")
        | line._3.contains("/html/wenda"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd10 = restRDD
      .filter(line => line._3.contains("/api/joke/lists") | line._3.contains("/api/jokes/add"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd11 = restRDD
      .filter(line => line._3.contains("/v6_getPlan") | line._3.contains("/html/planbook")
        | line._3.contains("/v6_company"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd12 = restRDD
      .filter(line => line._3.contains("/v6_read_columns") | line._3.contains("/v6_read_search")
        | line._3.contains("/weiyuedu"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd13 = restRDD
      .filter(line => line._3.contains("/api/listenDaily/item") | line._3.contains("/html/listendailyinfo"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd14 = restRDD
      .filter(line => line._3.contains("/address_book") | line._3.contains("/geturl")
        | line._3.contains("/getcards") | line._3.contains("/html/precustumer"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd15 = restRDD
      .filter(line => line._3.contains("/newchanpin"))
      .map(line => (line._1, line._4)).distinct().count

    val rdd16 = restRDD
      .filter(line => line._3.contains("/serverApi"))
      .map(line => (line._1, line._4)).distinct().count

    println(rdd1+"\n"+rdd2+"\n"+rdd3+"\n"+rdd4+"\n"+rdd5+"\n"+rdd6+"\n"+rdd7+"\n"+rdd8+"\n"+rdd9+"\n"+
      rdd10+"\n"+rdd11+"\n"+rdd12+"\n"+rdd13+"\n"+rdd14+"\n"+rdd15+"\n"+rdd16)

    spark.stop()
  }
}
