package com.prince.demo.spark.example

/**
  * sort的一些example
  * Created by princeping on 2017/5/23.
  */
object Sort {
  def main(args: Array[String]): Unit = {

    //val list = List(25, 57, 14, 88, 35, 61, 3, 7)
    //println(quickSort(list))

    //val list = List(1, 2, 3, 7, 12, 20, 23, 43)
    //println(mergedSort((x: Int, y: Int) => x < y)(list))

    val list = List(3, 12, 43, 23, 7, 1, 2, 20)
    println(bubbleSort(list))
  }

  /*快速排序*/
  def quickSort(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case List() => List()
    case head :: tail =>
      val (left, right) = tail.partition(_ < head)
      quickSort(left) ::: head :: quickSort(right)
  }


  /*并归排序*/
  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {
    //找到第一个最小的值，然后递归的进行比较（less只是一个def函数）
    def merged(xList: List[T], yList: List[T]): List[T] = {

      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          if (less(x, y)) x :: merged(xTail, yTail)
          else
          y :: merged(xList, yList)
        }
      }
    }
    val n = list.length / 2
    if(n == 0) list
    else {
      //splitAt从第n个元素开始切分，把数据分为2份
      val (x, y) = list splitAt n
      //合并左右数组
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }

  /**
    * 冒泡排序
    * 外层循环做拆分
    * 内层循环做排序
    */
  def bubbleSort(i: List[Int]): List[Int] = i match{
    case List() => List()
    case head :: tail => bSort(head, bubbleSort(tail))
  }
  def bSort(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if(data <= head) data :: dataSet else head :: bSort(data, tail)
  }

}
