package org.test.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount  {
  
  def main(args: Array[String]) {
  
  val conf = new SparkConf().setMaster("local[2]").setAppName("Word Count")
  
  val sc  = new SparkContext(conf)
  
  sc.setLogLevel("ERROR")
  
 // val linesRDD = sc.parallelize(List(1,2,3,4,5,6,6,7,8,9,10), 2)
  
  val linesRDD = sc.textFile("data.txt", 2)
  
  // println(linesRDD.count)
  
  val outRDD = linesRDD.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortBy(-_._2)
  
  outRDD.foreach(println)
  
 // outRDD.repartition(1).saveAsTextFile("dataOut2")  
  
  }
  
}