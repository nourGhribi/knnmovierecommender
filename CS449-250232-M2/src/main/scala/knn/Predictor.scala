package knn

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

  // Map of the MAE corresponding to each k when fitting knn
  // along with the Map of similarities and their intersection count
  val ksMAE = computeMae_M2_knn(train,test).mapValues(e => (round_double(e._1), e._2))

  // Get the lowest k for which the MAE is better than the baseline.
  val lowestK = ksMAE.mapValues(_._1 - MAE_BASELINE).filter(_._2<0).maxBy({case (k,kmae)=> kmae})

  // get the number of similarities stored for each k
  val intersections = ksMAE.mapValues(_._2.size)


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
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> ksMAE(10)._1, // Datatype of answer: Double
            "MaeForK=30" -> ksMAE(30)._1, // Datatype of answer: Double
            "MaeForK=50" -> ksMAE(50)._1, // Datatype of answer: Double
            "MaeForK=100" -> ksMAE(100)._1, // Datatype of answer: Double
            "MaeForK=200" -> ksMAE(200)._1, // Datatype of answer: Double
            "MaeForK=300" -> ksMAE(300)._1, // Datatype of answer: Double
            "MaeForK=400" -> ksMAE(400)._1, // Datatype of answer: Double
            "MaeForK=800" -> ksMAE(800)._1, // Datatype of answer: Double
            "MaeForK=943" -> ksMAE(943)._1, // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> lowestK._1, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> round_double(lowestK._2) // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> 8 * intersections(10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> 8 * intersections(30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" ->  8 * intersections(50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" ->  8 * intersections(100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" ->  8 * intersections(200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" ->  8 * intersections(300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> 8 * intersections(400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> 8 * intersections(800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> 8 * intersections(943) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> 8E9, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> math.floor(8E9 / (lowestK._1*3*8)).toLong// Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
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
