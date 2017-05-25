package com.prince.demo.spark.example

/**
  * Created by princeping on 2017/5/24.
  */
object Set {
  def main(args: Array[String]): Unit = {
    val numbers = Seq(11, 2, 5, 1, 6, 3, 9)
    println(numbers.max)
    println(numbers.min)
    println(numbers.filterNot(n => n % 2 == 0))
  }
}
