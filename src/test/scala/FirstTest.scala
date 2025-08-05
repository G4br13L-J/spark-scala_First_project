package de.gabriel.sparkvideocourse

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers._

import java.sql.Date

class FirstTest extends AnyFunSuite{

  private val spark = SparkSession.builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(Seq(
    StructField("date", DateType, nullable = true),
    StructField("open", DoubleType, nullable = true),
    StructField("close", DoubleType, nullable = true),
  ))

  test("Returns highest closing prices per year") {
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2022-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0),
    )

    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0),
    )

    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testDf = spark.createDataset(testRows)
    val actualRows = Main.highestClosingPricesPerYear(testDf)// Call the add method from the Main object
      .collect()

    actualRows should contain theSameElementsAs expected
  }
}
