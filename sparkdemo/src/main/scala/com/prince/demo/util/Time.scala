package com.prince.demo.util

import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.LocalDate
import java.util.{Calendar, Date}

/**
  * Created by princeping on 2017/4/27.
  */
object Time {
  def main(args: Array[String]): Unit = {

//    val nowdate = LocalDate.now()
//    println("今天是：" + LocalDate.now())
//    println("明天是：" + nowdate.plusDays(1))
//    println("昨天是：" + nowdate.minusDays(1))
//    println("今天是"+nowdate.getYear+"年的第"+nowdate.getDayOfYear+"天")
//    println("这个月有"+nowdate.getDayOfMonth+"天")
//    println("今天星期"+nowdate.getDayOfWeek)
//    println("这个月是"+nowdate.getMonth)

    //getNowDate()
    //getYesterday()
    //getNowWeekStart()
    //getNowWeekEnd()
    //getNowMonthStart()
    //getNowMonthEnd()
    getCoreTime("00:40:33", "00:50:39")
  }

  /*获取今天日期*/
  def getNowDate(): Unit ={
    val date:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val now = dateFormat.format(date)
    println(now)
  }

  /*获取昨天日期*/
  def getYesterday(): Unit ={
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    println(yesterday)
  }

  /*获取本周开始日期*/
  def getNowWeekStart(): Unit ={
    val cal:Calendar = Calendar.getInstance()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    val period = df.format(cal.getTime)
    println(period)
  }

  /*获取本周末日期*/
  def getNowWeekEnd(): Unit ={
    val cal:Calendar = Calendar.getInstance()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    cal.add(Calendar.WEEK_OF_YEAR, 1)
    val period = df.format(cal.getTime)
    println(period)
  }

  /*获取本月第一天日期*/
  def getNowMonthStart(): Unit ={
    val cal:Calendar = Calendar.getInstance()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    val period = df.format(cal.getTime)
    println(period)
  }

  /*获取本月最后一天*/
  def getNowMonthEnd(): Unit ={
    val cal:Calendar = Calendar.getInstance()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    val period = df.format(cal.getTime)
    println(period)
  }

  /*计算时间差*/
  def getCoreTime(start_time:String, end_time:String): Unit ={
    val df:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val begin:Date = df.parse(start_time)
    val end:Date = df.parse(end_time)
    val between:Long = (end.getTime() - begin.getTime())/1000
    //val hour:Float = between.toFloat/3600
    println(between)
  }

}
