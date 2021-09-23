package similarity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.rogach.scallop._
import utils._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

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

  //----------------------------------------------------------------

  val cosine_mae = computeMae_M2(train,test, "cosine")._1

  //----------------------------------------------------------------

  val (jaccard_mae, intersections) = computeMae_M2(train,test, "jaccard")

  //----------------------------------------------------------------

  val simCounts = math.pow(train.groupBy(_.user).count(),2) // |U|*|U|

  // This is the actual number of the similarities computed in this implementation.
  val modelSimCounts = train.groupBy(_.user).count()*test.groupBy(_.user).count()

  //----------------------------------------------------------------

  val intersectionsCount = intersections.map(_._2).toList


  val nonZerointersectionsCount = intersectionsCount.filter(_!=0)

  val (intersections_min, intersections_max, intersections_mean, intersections_stddev) = metrics(intersectionsCount.map(_.toDouble))

  //----------------------------------------------------------------

  val times = executionTimes(train,test,i=5)

  //----------------------------------------------------------------

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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> round_double(cosine_mae), // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> round_double(cosine_mae-utils.MAE_BASELINE) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> round_double(jaccard_mae), // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> round_double(jaccard_mae-cosine_mae) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> simCounts// Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> intersections_min,  // Datatype of answer: Double
              "max" -> intersections_max, // Datatype of answer: Double
              "average" -> round_double(intersections_mean), // Datatype of answer: Double
              "stddev" -> round_double(intersections_stddev) // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" ->  8 * nonZerointersectionsCount.length // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> times("cosine")._1,  // Datatype of answer: Double
              "max" -> times("cosine")._2, // Datatype of answer: Double
              "average" -> times("cosine")._3, // Datatype of answer: Double
              "stddev" -> times("cosine")._4 // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> times("similarity")._1,  // Datatype of answer: Double
              "max" -> times("similarity")._2, // Datatype of answer: Double
              "average" -> times("similarity")._3, // Datatype of answer: Double
              "stddev" -> times("similarity")._4 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> times("similarity")._3 / modelSimCounts , // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> round_double(times("similarity")._3 / times("cosine")._3) // Datatype of answer: Double
          )
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
