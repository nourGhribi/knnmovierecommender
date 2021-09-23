import org.json4s.jackson.Serialization
import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    //--------------------------------------------------------------
    val days_per_year = 365

    //----------ICC.M7---------------
    val ICCM7_buying_cost = 35000
    val ICCM7_renting_cost = 20.40
    val ICCM7RAM = 24*64

    val days_renting_ICCM7 = ICCM7_buying_cost/ICCM7_renting_cost
    val years_renting_ICCM7 = days_renting_ICCM7/days_per_year

    val daily_cost_container_ICCM7_CPU_throughput = (2*14*0.088)
    val daily_cost_container_ICCM7_RAM_throughput = (ICCM7RAM*0.012)
    val daily_cost_container_ICCM7 = daily_cost_container_ICCM7_CPU_throughput +
                                      daily_cost_container_ICCM7_RAM_throughput
    val ratio_ICCM7_container_ICCM7 = ICCM7_renting_cost / daily_cost_container_ICCM7

    //----------4 RPi----------
    /***
     * A throughput of 1 Intel CPU core is approx 4 RPis throughputs and 1 vCPU
     ***/
    val RPi_RAM = 8.0
    val RPI_CPU_Throuhput = 0.25

    val container_raspberrypi_ram_cost = 4*RPi_RAM*0.012
    val container_raspberrypi_cpu_cost = 4*RPI_CPU_Throuhput*0.088

    val container_4rpi_cost = container_raspberrypi_ram_cost+container_raspberrypi_cpu_cost

    val cost_raspberrypi_max = 4 * 0.054
    val cost_raspberrypi_min = 4 * 0.0108

    val ratio_rpi_container_rpi = cost_raspberrypi_max / container_4rpi_cost

    val cost_RPi = 94.83
    val cost_buying_4RPi4s = 4 * cost_RPi

    val days_container_exp_4RPis_min = math.ceil(cost_buying_4RPi4s/(container_4rpi_cost-cost_raspberrypi_min))
    val days_container_exp_4RPis_max = math.ceil(cost_buying_4RPi4s/(container_4rpi_cost-cost_raspberrypi_max))


    //----------- ICCM7 vs RPIS -----------
    val raspberryPis_for_ICCM7 = math.floor(ICCM7_buying_cost/cost_RPi)

    //----------- Users in Memory -----------
    /**
     * Each platform should leave 50% of memory off
     */
    val (simMem, ratingMem) = (4.0, 1.0)
    val usersMemory = (200.0*simMem)+(100.0 * ratingMem)
    val usersPerGB = math.floor(0.5E9 / usersMemory)
    val usersPerRPi = math.floor((0.5E9 / usersMemory) * RPi_RAM)
    val usersPerICCM7 = math.floor((0.5E9 / usersMemory) * ICCM7RAM)

    //--------------------------------------------------------------
    var conf = new Conf(args)

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
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> math.ceil(days_renting_ICCM7), // Datatype of answer: Doubles
              "MinYearsOfRentingICC.M7" -> years_renting_ICCM7 // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> daily_cost_container_ICCM7, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> ratio_ICCM7_container_ICCM7, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> (ratio_ICCM7_container_ICCM7 > 1.05) // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> container_4rpi_cost, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> cost_raspberrypi_max/container_4rpi_cost, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> cost_raspberrypi_min/container_4rpi_cost, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> (ratio_rpi_container_rpi > 1.05) // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> days_container_exp_4RPis_min, // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> days_container_exp_4RPis_max // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> raspberryPis_for_ICCM7, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> (raspberryPis_for_ICCM7/4)/28, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> (raspberryPis_for_ICCM7*8)/(24*64) // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> usersPerGB, // Datatype of answer: Double
              "NbUserPerRPi" -> usersPerRPi, // Datatype of answer: Double
              "NbUserPerICC.M7" -> usersPerICCM7 // Datatype of answer: Double
            )
            // Answer the Question 5.1.7 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
