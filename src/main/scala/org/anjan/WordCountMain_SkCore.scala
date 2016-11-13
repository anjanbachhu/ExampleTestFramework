package org.anjan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator}

object WordCountMain_SkCore {

  private val master = "local[2]"
  private val appName = "example-spark"
  private val stopWords = Set("a", "an", "the")

  def main(args: Array[String]): Unit = {

    // To disable/enable the Logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/data/words.txt")

    val wordsCount = WordCountDAO.count(sc, lines, stopWords)
    //wordsCount.saveAsTextFile("src/main/resources/data/results/wordsResults")

    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info(wordsCount.collect().mkString("[", ", ", "]"))
  }
}
