package org.anjan

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.immutable.Stream.Empty

class WordCountTestSpec_SkCore extends FlatSpec with SparkContextTestSpec with GivenWhenThen with Matchers {

  behavior of "WordCount App development"

  it should "not blank" in {
    Given("Data Set")
    val lines = Array("")

    When("count words")
    val wordCounts = WordCountDAO.count(sc, sc.parallelize(lines)).collect()

    Then("")
    wordCounts shouldBe Empty
  }

  it should "contain words" in {
  }
}


