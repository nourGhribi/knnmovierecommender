package predict

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.rogach.scallop._
import utils.Rating

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

//case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
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
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

  /* -------- processing code -------- */

  /* ----- Global Average Method ----- */
  val globalMae = utils.computeMae(train,test,"Global")
  /* -------------------------------- */

  /* ----- User Average Method ----- */
  val userMae = utils.computeMae(train,test,"User")
  /* -------------------------------- */


  /* ----- Items Average Method ----- */
  val itemMae = utils.computeMae(train,test,"Item")
  /* -------------------------------- */

  /* ----- Baseline Model Method ----- */
  val baselineMae = utils.computeMae(train,test,"Baseline")
  /* --------------------------------- */

  /* execution times */
  val executionTimeMap = utils.executionTimes(train,test)
  val globalMetrics = executionTimeMap("Global")
  val userMetrics = executionTimeMap("User")
  val itemMetrics = executionTimeMap("Item")
  val baselineMetrics = executionTimeMap("Baseline")

  /* --------------------------------- */
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
          "Q3.1.4" -> Map(
            "MaeGlobalMethod" -> globalMae, // Datatype of answer: Double
            "MaePerUserMethod" -> userMae, // Datatype of answer: Double
            "MaePerItemMethod" -> itemMae, // Datatype of answer: Double
            "MaeBaselineMethod" -> baselineMae // Datatype of answer: Double
          ),

          "Q3.1.5" -> Map(
            "DurationInMicrosecForGlobalMethod" -> Map(
              "min" -> globalMetrics._1,  // Datatype of answer: Double
              "max" -> globalMetrics._2,  // Datatype of answer: Double
              "average" -> globalMetrics._3, // Datatype of answer: Double
              "stddev" -> globalMetrics._4 // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerUserMethod" -> Map(
              "min" -> userMetrics._1,  // Datatype of answer: Double
              "max" -> userMetrics._2, // Datatype of answer: Double
              "average" -> userMetrics._3, // Datatype of answer: Double
              "stddev" -> userMetrics._4 // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerItemMethod" -> Map(
              "min" -> itemMetrics._1,  // Datatype of answer: Double
              "max" -> itemMetrics._2,  // Datatype of answer: Double
              "average" -> itemMetrics._3, // Datatype of answer: Double
              "stddev" -> itemMetrics._4 // Datatype of answer: Double
            ),
            "DurationInMicrosecForBaselineMethod" -> Map(
              "min" -> baselineMetrics._1,  // Datatype of answer: Double
              "max" -> baselineMetrics._2, // Datatype of answer: Double
              "average" -> baselineMetrics._3, // Datatype of answer: Double
              "stddev" -> baselineMetrics._4 // Datatype of answer: Double
            ),
            "RatioBetweenBaselineMethodAndGlobalMethod" -> (baselineMetrics._3 / globalMetrics._3) // Datatype of answer: Double
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

