package com.prince.demo.spark.mytest

import org.apache.spark.sql.SparkSession

/**
  * 计算PV
  * Created by princeping on 2017/5/16.
  */
object PV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("FilterDemo").getOrCreate()
    val input = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\新建文件夹\\0501.log")

    val restRDD = input.filter(line => line.split("\\|")(3).contains("rest-01.bxd365.com"))

    val rest0 = restRDD.filter(line => line.split("\\|")(6).contains("/getWelcomeImages")).count()
    val rest1 = restRDD.filter(line => line.split("\\|")(6).contains("/login")).count()
    val rest2 = restRDD.filter(line => line.split("\\|")(6).contains("/V6_getIndexad")).count()
    val rest3 = restRDD.filter(line => line.split("\\|")(6).contains("/api/showPlanbook/check")).count()
    val rest4 = restRDD.filter(line => line.split("\\|")(6).contains("/activity_log")).count()
    val rest5 = restRDD.filter(line => line.split("\\|")(6).contains("/api/listenDaily/itemlist")).count()
    val rest6 = restRDD.filter(line => line.split("\\|")(6).contains("/api/indexs/navigation")).count()
    val rest7 = restRDD.filter(line => line.split("\\|")(6).contains("/bxdInsurProduct/lists")).count()
    val rest8 = restRDD.filter(line => line.split("\\|")(6).contains("/api/elite/lists")).count()//TODO
    val rest9 = restRDD.filter(line => line.split("\\|")(6).contains("/api/weiyuedu/lists")).count()//TODO
    val rest10 = restRDD.filter(line => line.split("\\|")(6).contains("/api/clause")).count()
    val rest11 = restRDD.filter(line => line.split("\\|")(6).contains("/api/weiyuedu")).count()
    val rest12 = restRDD.filter(line => line.split("\\|")(6).contains("/html/singlePage/diamondInfo")).count()
    val rest13 = restRDD.filter(line => line.split("\\|")(6).contains("/api/diamond/serveplatefrom")).count()
    val rest14 = restRDD.filter(line => line.split("\\|")(6).contains("/html/Product/baoshi/index")).count()
    val rest15 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_getConch")).count()
    val rest16 = restRDD.filter(line => line.split("\\|")(6).contains("/html/weizhanpage/card?")).count()
    val rest17 = restRDD.filter(line => line.split("\\|")(6).contains("/api/Weizhan/microCard")).count()
    val rest18 = restRDD.filter(line => line.split("\\|")(6).contains("/wallet")).count()
    val rest19 = restRDD.filter(line => line.split("\\|")(6).contains("/api/Conch/mylog")).count()
    val rest20 = restRDD.filter(line => line.split("\\|")(6).contains("/conchshop/state")).count()
    val rest21 = restRDD.filter(line => line.split("\\|")(6).contains("/getInsureTags")).count()
    val rest22 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rhelper/Certificate")).count()
    val rest23 = restRDD.filter(line => line.split("\\|")(6).contains("/company")).count()
    val rest24 = restRDD.filter(line => line.split("\\|")(6).contains("/modify_my_information")).count()
    val rest25 = restRDD.filter(line => line.split("\\|")(6).contains("/upload_avatar")).count()
    val rest26 = restRDD.filter(line => line.split("\\|")(6).contains("/realname_certification")).count()
    val rest27 = restRDD.filter(line => line.split("\\|")(6).contains("/MyMrfavq")).count()
    val rest28 = restRDD.filter(line => line.split("\\|")(6).contains("/api/collect/my")).count()
    val rest29 = restRDD.filter(line => line.split("\\|")(6).contains("/my_plans")).count()
    val rest30 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_feedback")).count()
    val rest31 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_myfeedback")).count()
    val rest32 = restRDD.filter(line => line.split("\\|")(6).contains("/forget_password")).count()
    val rest33 = restRDD.filter(line => line.split("\\|")(6).contains("/change_user_identity")).count()
    val rest34 = restRDD.filter(line => line.split("\\|")(6).contains("/api/Weizhan")).count()
    val rest35 = restRDD.filter(line => line.split("\\|")(6).contains("/getMyProducts")).count()
    val rest36 = restRDD.filter(line => line.split("\\|")(6).contains("/api/commonIntface/adlist")).count()
    val rest37 = restRDD.filter(line => line.split("\\|")(6).contains("/api/baoxianVideo/clist")).count()
    val rest38 = restRDD.filter(line => line.split("\\|")(6).contains("/api/baoxianVideo/itemlist")).count()
    val rest39 = restRDD.filter(line => line.split("\\|")(6).contains("/api/baoxianVideo/agent")).count()
    val rest40 = restRDD.filter(line => line.split("\\|")(6).contains("/api/commonIntface/historyAdd")).count()
    val rest41 = restRDD.filter(line => line.split("\\|")(6).contains("/api/comment/publiclist")).count()
    val rest42 = restRDD.filter(line => line.split("\\|")(6).contains("/api/baoxianVideo/exchange")).count()
    val rest43 = restRDD.filter(line => line.split("\\|")(6).contains("/api/commonIntface/historyDel")).count()
    val rest44 = restRDD.filter(line => line.split("\\|")(6).contains("/api/wenda/signNum")).count()//TODO
    val rest45 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/list")).count()
    val rest46 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/catenum")).count()
    val rest47 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_getTaskList")).count()//TODO
    val rest48 = restRDD.filter(line => line.split("\\|")(6).contains("/getReward")).count()
    val rest49 = restRDD.filter(line => line.split("\\|")(6).contains("/api/wenda/initiateChat")).count()
    val rest50 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/keysearch")).count()
    val rest51 = restRDD.filter(line => line.split("\\|")(6).contains("/api/commonIntface/multiUserInfo")).count()
    val rest52 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/doneQuestion")).count()
    val rest53 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/myfavq")).count()
    val rest54 = restRDD.filter(line => line.split("\\|")(6).contains("/rest.php?r=rww/getanswers")).count()
    val rest55 = restRDD.filter(line => line.split("\\|")(6).contains("/api/wenda/initiateGrade")).count()
    val rest56 = restRDD.filter(line => line.split("\\|")(6).contains("/api/wenda/rankInfo")).count()
    val rest57 = restRDD.filter(line => line.split("\\|")(6).contains("/api/wenda/rankList")).count()
    val rest58 = restRDD.filter(line => line.split("\\|")(6).contains("/api/joke/lists")).count()
    val rest59 = restRDD.filter(line => line.split("\\|")(6).contains("/api/jokes/add")).count()
    val rest60 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_getPlan")).count()
    val rest61 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_read_columns")).count()
    val rest62 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_read_search")).count()
    val rest63 = restRDD.filter(line => line.split("\\|")(6).contains("/api/listenDaily/item")).count()
    val rest64 = restRDD.filter(line => line.split("\\|")(6).contains("/api/collect/add")).count()
    val rest65 = restRDD.filter(line => line.split("\\|")(6).contains("/api/collect/del")).count()
    val rest66 = restRDD.filter(line => line.split("\\|")(6).contains("/api/commonIntface/historyAdd")).count()
    val rest67 = restRDD.filter(line => line.split("\\|")(6).contains("/api/liker/add")).count()
    val rest68 = restRDD.filter(line => line.split("\\|")(6).contains("/vcode")).count()
    val rest69 = restRDD.filter(line => line.split("\\|")(6).contains("/forget_password_vcode")).count()
    val rest70 = restRDD.filter(line => line.split("\\|")(6).contains("/api/comment/add")).count()
    val rest71 = restRDD.filter(line => line.split("\\|")(6).contains("/addInsureTags")).count()
    val rest72 = restRDD.filter(line => line.split("\\|")(6).contains("/api/my/modifySettings")).count()
    val rest73 = restRDD.filter(line => line.split("\\|")(6).contains("/weiyuedu")).count()
    val rest74 = restRDD.filter(line => line.split("\\|")(6).contains("/v6_company")).count()
    val rest75 = restRDD.filter(line => line.split("\\|")(6).contains("/html/listendailyinfo")).count()
    val rest76 = restRDD.filter(line => line.split("\\|")(6).contains("/address_book")).count()
    val rest77 = restRDD.filter(line => line.split("\\|")(6).contains("/geturl")).count()
    val rest78 = restRDD.filter(line => line.split("\\|")(6).contains("/getcards")).count()
    val rest79 = restRDD.filter(line => line.split("\\|")(6).contains("/ufiles")).count()
    val rest80 = restRDD.filter(line => line.split("\\|")(6).contains("/MrFavq")).count()
    val rest81 = restRDD.filter(line => line.split("\\|")(6).contains("/newchanpin")).count()
    val rest82 = restRDD.filter(line => line.split("\\|")(6).contains("/html/baoshiApi /")).count()
    val rest83 = restRDD.filter(line => line.split("\\|")(6).contains("/weizhan")).count()
    val rest84 = restRDD.filter(line => line.split("\\|")(6).contains("/html/baoxianVideo")).count()
    val rest85 = restRDD.filter(line => line.split("\\|")(6).contains("/html/wenda")).count()
    val rest86 = restRDD.filter(line => line.split("\\|")(6).contains("/html/planbook")).count()
    val rest87 = restRDD.filter(line => line.split("\\|")(6).contains("/html/precustumer")).count()
    val rest88 = restRDD.filter(line => line.split("\\|")(6).contains("/serverApi")).count()

    println(rest0 +"\n"
      +rest1 +"\n"
      +(rest2+rest3+rest4+rest5+rest6+rest7)/6+"\n"
      +((rest8+rest9)/2 +rest10+rest11)+"\n"
      +(rest12+rest13+rest14+rest82)+"\n"
      +(rest15+rest16+rest17+rest18+rest19+rest20+rest21+rest22+rest23+rest24+rest25+rest26+rest27+rest28+rest29+rest30+rest31+rest33+rest71+rest72)+"\n"
      +(rest34+rest35+rest83) +"\n"
      +(rest36+rest37+rest38+rest39+rest40+rest41+rest42+rest43+rest84)+"\n"
      +((rest44+rest45+rest46+rest47)/4 +rest48+rest49+rest50+rest51+rest52+rest53+rest54+rest55+rest56+rest57+rest85)+"\n"
      +(rest58+rest59)+"\n"
      +(rest74+rest60+rest86)+"\n"
      +(rest61+rest62+rest73)+"\n"
      +(rest63+rest75)+"\n"
      +(rest76+rest77+rest78+rest87)+"\n"
      +rest81+"\n"
      +rest88+"\n"
      +(rest64+rest65+rest66+rest67+rest68+rest69+rest70+rest79+rest80+rest32)+"\n"
      +restRDD.count())
  }
}
