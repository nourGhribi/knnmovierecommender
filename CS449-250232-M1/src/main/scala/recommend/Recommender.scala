package recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.rogach.scallop._
import utils._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}


object Recommender extends App {
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

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())


  //My user ID
  val userID = 944

  // Items with no rating present will get default value -1.0

  val personalData = personalFile.map(l => {
    val cols = l.split(";").map(_.trim)
    cols.length match {
      case 2 => {
        val item = cols(0).toInt
        val title = cols(1)
        val rating = -1.0
        Rating(userID, item, rating)
      }
      case _ => {
        val item = cols(0).toInt
        val title = cols(1)
        val rating = cols(2).toDouble
        Rating(userID, item, rating)
      }
    }
    })

  val movieTitles = personalFile.map(l => {
    val cols = l.split(";").map(_.trim)
    val id = cols(0).toInt
    val title = cols(1)
    (id,title)
  })

  assert(personalFile.count == 1682, "Invalid personal data")

  /* preprocessing */
  val filteredData = personalData.filter(r=>r.rating!=(-1.0)) //Personal rated movies only
  val moviesData = data ++ filteredData // Train data
  val testData = personalData.filter(r=>r.rating==(-1.0)) // test data (Movies that don't have ratings)

  /* ---- Q.4.1.1 ---- */
  // Using the train data and test data to fit the model and predict ratings
  val predictions = baseline(moviesData,testData)
  // Get the top 5 rated movies using predictions
  val top5MoviesList = top5Movies(predictions,movieTitles)

  /* ---- Q.4.1.2 Bonus ---- */
  // Fit the model and predict
  val predictions2 = myModel(moviesData,testData, 400)
  // Get the top 5 rated movies using predictions
  val top5Movies2List = top5Movies(predictions2,movieTitles)


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

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.

            "Q4.1.1" ->
              top5MoviesList,
            "Q4.1.2" -> top5Movies2List
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
