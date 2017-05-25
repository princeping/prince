package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * Created by princeping on 2017/5/3.
  */
object DemoTwo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("FilterDemo").getOrCreate()
    val input = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\430.log")

    val restRDD = input.filter(line => line.split("\\|")(3).contains("rest-01.bxd365.com")
      & line.split("\\|")(6).contains("/api/forward/add").equals(false)
      & line.split("\\|")(6).contains("/check_version").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/newMessage").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/actionTrigger").equals(false)
      & line.split("\\|")(6).contains("/api/jokes/listtop").equals(false)
      & line.split("\\|")(6).contains("/api/index/navigation").equals(false)
      & line.split("\\|")(6).contains("/getcommonkeywords").equals(false))

    val filterRDD = restRDD.filter(line => line.split("\\|")(6).contains("/getWelcomeImages").equals(false)
      & line.split("\\|")(6).contains("/login").equals(false)
      & line.split("\\|")(6).contains("/V_getIndexad").equals(false)
      & line.split("\\|")(6).contains("/api/showPlanbook/check").equals(false)
      & line.split("\\|")(6).contains("/activity_log").equals(false)
      & line.split("\\|")(6).contains("/api/listenDaily/itemlist").equals(false)
      & line.split("\\|")(6).contains("/api/indexs/navigation").equals(false)
      & line.split("\\|")(6).contains("/bxdInsurProduct/lists").equals(false)
      & line.split("\\|")(6).contains("/Api/elite/lists").equals(false)//TODO
      & line.split("\\|")(6).contains("/api/weiyuedu/lists").equals(false)//TODO
      & line.split("\\|")(6).contains("/Api/clause").equals(false)
      & line.split("\\|")(6).contains("/Api/weiyuedu").equals(false)
      & line.split("\\|")(6).contains("/Html/singlePage/diamondInfo").equals(false)
      & line.split("\\|")(6).contains("/api/diamond/serveplatefrom").equals(false)
      & line.split("\\|")(6).contains("/html/Product/baoshi/index").equals(false)
      & line.split("\\|")(6).contains("/v_getConch").equals(false)
      & line.split("\\|")(6).contains("/html/weizhanpage/card?").equals(false)
      & line.split("\\|")(6).contains("/api/Weizhan/microCard").equals(false)
      & line.split("\\|")(6).contains("/wallet").equals(false)
      & line.split("\\|")(6).contains("/api/Conch/mylog").equals(false)
      & line.split("\\|")(6).contains("/conchshop/state").equals(false)
      & line.split("\\|")(6).contains("/getInsureTags").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rhelper/Certificate").equals(false)
      & line.split("\\|")(6).contains("/company").equals(false)
      & line.split("\\|")(6).contains("/modify_my_information").equals(false)
      & line.split("\\|")(6).contains("/upload_avatar").equals(false)
      & line.split("\\|")(6).contains("/realname_certification").equals(false)
      & line.split("\\|")(6).contains("/MyMrfavq").equals(false)
      & line.split("\\|")(6).contains("/api/collect/my").equals(false)
      & line.split("\\|")(6).contains("/my_plans").equals(false)
      & line.split("\\|")(6).contains("/v_feedback").equals(false)
      & line.split("\\|")(6).contains("/v_myfeedback").equals(false)
      & line.split("\\|")(6).contains("/forget_password").equals(false)
      & line.split("\\|")(6).contains("/change_user_identity").equals(false)
      & line.split("\\|")(6).contains("/api/Weizhan").equals(false)
      & line.split("\\|")(6).contains("/getMyProducts").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/adlist").equals(false)
      & line.split("\\|")(6).contains("/api/baoxianVideo/clist").equals(false)
      & line.split("\\|")(6).contains("/api/baoxianVideo/itemlist").equals(false)
      & line.split("\\|")(6).contains("/api/baoxianVideo/agent").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/historyAdd").equals(false)
      & line.split("\\|")(6).contains("/api/comment/publiclist").equals(false)
      & line.split("\\|")(6).contains("/api/baoxianVideo/exchange").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/historyDel").equals(false)
      & line.split("\\|")(6).contains("/api/wenda/signNum").equals(false)//TODO
      & line.split("\\|")(6).contains("/rest.php?r=rww/list").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rww/catenum").equals(false)
      & line.split("\\|")(6).contains("/v_getTaskList").equals(false)//TODO
      & line.split("\\|")(6).contains("/getReward").equals(false)
      & line.split("\\|")(6).contains("/api/wenda/initiateChat").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rww/keysearch").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/multiUserInfo").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rww/doneQuestion").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rww/myfavq").equals(false)
      & line.split("\\|")(6).contains("/rest.php?r=rww/getanswers").equals(false)
      & line.split("\\|")(6).contains("/api/wenda/initiateGrade").equals(false)
      & line.split("\\|")(6).contains("/api/wenda/rankInfo").equals(false)
      & line.split("\\|")(6).contains("/api/wenda/rankList").equals(false)
      & line.split("\\|")(6).contains("/api/joke/lists").equals(false)
      & line.split("\\|")(6).contains("/api/jokes/add").equals(false)
      & line.split("\\|")(6).contains("/v_getPlan").equals(false)
      & line.split("\\|")(6).contains("/v_read_columns").equals(false)
      & line.split("\\|")(6).contains("/v_read_search").equals(false)
      & line.split("\\|")(6).contains("/api/listenDaily/item").equals(false)
      & line.split("\\|")(6).contains("/api/collect/add").equals(false)
      & line.split("\\|")(6).contains("/api/collect/del").equals(false)
      & line.split("\\|")(6).contains("/api/commonIntface/historyAdd").equals(false)
      & line.split("\\|")(6).contains("/api/liker/add").equals(false)
      & line.split("\\|")(6).contains("/vcode").equals(false)
      & line.split("\\|")(6).contains("/forget_password_vcode").equals(false)
      & line.split("\\|")(6).contains("/api/comment/add").equals(false))

    filterRDD.saveAsTextFile("E:\\filter")
  }
}
