package weather

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

import sys.process._
import java.net.URL
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.io.FileUtils

import scala.io.Source



// Structure to capture the weather data of the stations
case class WeatherData(station : String, year: Int, month: Int, tmax: Option[Double], tmin: Option[Double], af: Option[Int], rain: Option[Double], sunshine: Option[Double]) extends Serializable


/** The main class */
object app extends Weather {

  @transient lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("app")
      .config("spark.master", "local")
      .getOrCreate()
  import spark.implicits._


  /** Main function  */
  def main(args: Array[String]): Unit = {


      FileUtils.deleteDirectory(new File("/Users/omprakash/IdeaProjects/src/main/resources/original/*"))
      FileUtils.deleteDirectory(new File("/Users/omprakash/IdeaProjects/src/main/resources/modified/*"))


     createOriginalAndModifiedData

    val datasetAsString   = spark.read.textFile(s"/Users/omprakash/IdeaProjects/src/main/resources/modified/*").as[String]
    val datasetAsWeatherData    = convertToWeatherData(datasetAsString.rdd)
    val dataFrame = datasetAsWeatherData.toDF()

    rankByMeasures(dataFrame)


    rankByAverageRain(dataFrame)

    rankByAverageSunshine(dataFrame)

    timeOfWorstRainfall(dataFrame)


    averageMayAllStations(dataFrame)


    spark.stop()
  }

  private def averageMayAllStations(dataFrame: DataFrame) = {
    val df5 = dataFrame.groupBy($"month").agg(avg($"rain").alias("rain"), avg($"sunshine").alias("sunshine")).where($"month" === 5).select($"*").show()
  }

  private def timeOfWorstRainfall(dataFrame: DataFrame) = {
    val df4 = dataFrame.groupBy($"station").agg(min($"rain").alias("rain")).select($"*")
    val data_joined = dataFrame.join(df4, List("station", "rain")).show()
  }

  private def rankByAverageSunshine(dataFrame: DataFrame) = {
    val df3 = dataFrame.groupBy($"station").agg(avg($"sunshine").alias("avgSun")).select($"*")
    val rankTest3 = df3.withColumn("rank", rank().over(Window.orderBy($"avgSun".desc))).show()
  }

  private def rankByAverageRain(dataFrame: DataFrame) = {
    val df2 = dataFrame.groupBy($"station").agg(avg($"rain").alias("avgRain")).select($"*")
    val rankTest2 = df2.withColumn("rank", rank().over(Window.orderBy($"avgRain".desc))).show()
  }

  private def rankByMeasures(dataFrame: DataFrame) = {
    val df1 = dataFrame.groupBy($"station").agg(count($"station").alias("totalMeasures")).select($"*")
    val rankTest1 = df1.withColumn("rank", rank().over(Window.orderBy($"totalMeasures".desc))).show()
  }

  private def createOriginalAndModifiedData = {
    for (station <- stations) {

      new URL("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/" + station + "data.txt") #> new File("/Users/omprakash/IdeaProjects/src/main/resources/original/" + station + "data.txt") !!

      val file = new File("/Users/omprakash/IdeaProjects/src/main/resources/modified/" + station + "data.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      for (line <- Source.fromFile(s"/Users/omprakash/IdeaProjects/src/main/resources/original/" + station + "data.txt").getLines.drop(1)) {
        if (line.contains("Location") || line.contains("Estimated") || line.contains("Missing") || line.contains("Sunshine") ||
          line.contains("yyyy") || line.contains("Lat") || line.contains("degC") || line.contains("Site closed") || line.contains("Site Closed") || line.contains("Siteclosed")) {

        } else {
          val regex = "\\s+".r
          bw.write(station)
          bw.write(",")
          bw.write(regex.replaceAllIn(line.replaceAll("Change to Monckton Ave", ""), ",").replaceFirst(",", "").replaceAll("\\*", "").replaceAll("\\#", ""))
          bw.newLine()
        }

      }
      bw.close()
    }
  }
}


/** The parsing and means methods */
class Weather extends Serializable {

  /** Languages */
  val stations =
    List(
      "aberporth", "armagh", "ballypatrick", "bradford", "braemar", "camborne", "cambridge", "cardiff",
      "chivenor", "cwmystwyth", "dunstaffnage", "durham", "eastbourne", "eskdalemuir", "heathrow",
      "hurn", "lerwick", "leuchars"/*, "lowestoft"*/, "manston", "nairn", "newtonrigg",
      "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton",
      "stornoway", "suttonbonington", "tiree", "valley", "waddington"/*, "whitby"*/, "wickairport", "yeovilton")


  /** Load postings from the given file */
  def convertToWeatherData(lines: RDD[String]): RDD[WeatherData] = {

    lines.map(line => {
      val arr = line.split(",")
      //print(line)
      WeatherData(
        station  = arr(0).trim,
        year = arr(1).trim.toInt,
        month = arr(2).trim.toInt,
        tmax = if (arr(3).trim == "---") None else Some(arr(3).trim.toDouble),
        tmin = if (arr(4).trim == "---") None else Some(arr(4).trim.toDouble),
        af = if (arr(5).trim == "---") None else Some(arr(5).trim.toInt),
        rain = if (arr(6).trim == "---") None else Some(arr(6).trim.toDouble),
        sunshine = if (arr(7).trim== "---") None else Some(arr(7).trim.toDouble)

      )
    })

  }

}

