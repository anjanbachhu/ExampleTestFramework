package org.anjan

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level

object test{
  def main(args: Array[String]) {
    //val master = "local[*]"
    //val master = "spark://localhost:4040"
    val master = "local[2]"
    val appName = "Word Count 3 Example"
    val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(master))

    // To disable/enable the Logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFile = "file:/C:/Spark/Sample_Data/employeeData.txt"
    //val inputFile = "hdfs://127.0.0.1:8020/demo/data/Anjan/employeeData.txt"
    val wcRDD = sc.textFile(inputFile)

    //Evaluating purpose - split by space
    val SplitSpaces = wcRDD.flatMap(line => line.split(" "))
    //SplitSpaces.foreach(println)
    SplitSpaces.cache

    //Split by spaces and map the words
    val mapTheWords = wcRDD.flatMap(line => line.split(" ")).map(word => (word,1))
    //mapTheWords.foreach(println)

    //Count the words
    val groupsCounts = wcRDD.flatMap(line => line.split(" ")).map(word => (word,1)).groupByKey()
    val wordsCounts = wcRDD.flatMap(line => line.split(" ")).map(word => (word,1)).countByKey()
    //val wordsCounts = wcRDD.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b) => a + b)
    //val wordsCounts = wcRDD.flatMap(line => line.split(" ")).map(word => (word,1)).foldByKey(0)((a, b) => a + b)

    //groupsCounts.foreach(println)
    wordsCounts.foreach(println)

    //val op1 = wordsCounts.lookup("Vishal")
    //op1.foreach(println)

    //wordsCounts.collectAsMap().foreach(println)
    //wordsCounts.mapValues(a=>a/2).foreach(println)

    /*
     * Lines below are used to demonstrate
     * the facility of of common number RDD functions
     */
    val doubRDD = sc.parallelize(Seq(1.0,2.0,3.0,4.0,5.0,6.0))
    //doubRDD.foreach(println)
    println("mean of elements:-"+ doubRDD.mean+" sum of elements:- "+doubRDD.sum+ " variance of elements:- "+doubRDD.variance)
    println("total stats:-> "+doubRDD.stats)

  }
}
