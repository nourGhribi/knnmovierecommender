package stats

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.rogach.scallop._
import utils._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

//case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  data.map( r => (r.user,r.rating))


  /*----- processing code -----*/

  // Q3.1.1 -----------------------------------------------
  // ------------------- Global Average -------------------
  val globAvgRating = computeGlobalAverage(data)


  // Q3.1.2 -----------------------------------------------
  // ------------------- User Averages --------------------

  val averagesByUser = calculateAverageBykey(data,"User")

  // Average of user average ratings
  val avgAvgByUser = calculateAverageOfAverages(averagesByUser)

  // Min average of user ratings
  val minRatingPerUser = averagesByUser.map(r => r._2).min()

  // Max average of user ratings
  val maxRatingPerUser = averagesByUser.map(r => r._2).max()

  // The ratio of average ratings per user close to global average
  val ratioAvgRatingLowDevUsers = ratioDeviated(averagesByUser, globAvgRating, 0.5)

  // Q3.1.3 ------------------------------------------------
  // ------------------- Item Averages ---------------------

  val averageByItem = calculateAverageBykey(data, "Item")

  // average ratings of item average ratings
  val avgAvgByItem = calculateAverageOfAverages(averageByItem)

  // Min average of ratings by item
  val minRatingPerItem = averageByItem.map(r => r._2).min()

  // Max average of ratings by item
  val maxRatingPerItem = averageByItem.map(r => r._2).max()

  // The ratio of average ratings per item close to global average
  val ratioAvgRatingLowDevItems = ratioDeviated(averageByItem, globAvgRating, 0.5)


  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> globAvgRating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> minRatingPerUser,  // Datatype of answer: Double
                "max" -> maxRatingPerUser, // Datatype of answer: Double
                "average" -> avgAvgByUser // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratioAvgRatingLowDevUsers // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> minRatingPerItem,  // Datatype of answer: Double
                "max" -> maxRatingPerItem, // Datatype of answer: Double
                "average" ->  avgAvgByItem// Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratioAvgRatingLowDevItems // Datatype of answer: Double
          ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()

}
