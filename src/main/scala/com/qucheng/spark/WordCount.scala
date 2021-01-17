package com.qucheng.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val sparkConf = new SparkConf().setAppName("WordCount")

  val sparkContext = new SparkContext(sparkConf)

  val file = sparkContext.textFile("hdfs://172.16.4.151:8020/LICENSE")

  val words = file.flatMap(_.split(" "))

  val wordsTuple = words.map((_,1))

  wordsTuple.reduceByKey(_+_).saveAsTextFile("hdfs://172.16.4.151:8020/out1")

  sparkContext.stop()


}
