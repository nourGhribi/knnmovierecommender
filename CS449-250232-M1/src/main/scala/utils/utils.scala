import org.apache.spark.rdd.RDD

// The object utils will contain aal the necessary functions to the computations of the statistics.
package object utils {
  /**
   * Rating is the type of the input data.
   * Rating(user, item, rating)
   * user: Int , the user id
   * item: Int, the item movie id
   * rating: Double, the rating of user on item
   */
  case class Rating(user: Int, item: Int, rating: Double)

  /**
   * Computes the global average of ratings in data.
   * @param data RDD[(Rating)]
   * @return Average: Double
   */
  def computeGlobalAverage(data: RDD[Rating]): Double =
    data.map(r => r.rating).mean()

  /**
   * Computes the average with respect to the key, key can be "user" or "item".
   * Returns an RDD of averages by keys. (key, average).
   * @param data RDD[(Rating)]
   * @param key String
   * @return RDD[(Int, Double)]
   */
  def calculateAverageBykey(data: RDD[Rating], key: String): RDD[(Int, Double)] = {
    key.toLowerCase match {
      case "user" => data.map(r => (r.user,r.rating)).aggregateByKey((0.0,0))(
        (accumulator,element) => (accumulator._1+element, accumulator._2+1),
        (accumulator1,accumulator2) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)
      ).map(r => (r._1,r._2._1/r._2._2.toDouble))

      case "item" => data.map(r => (r.item,r.rating)).aggregateByKey((0.0,0))(
        (accumulator,element) => (accumulator._1+element, accumulator._2+1),
        (accumulator1,accumulator2) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)
      ).map(r => (r._1,r._2._1/r._2._2.toDouble))
    }
  }


  /**
   * Calculates the average of the AverageRating in data
   * @param data RDD[(key, AverageRating)]
   * @return Average of the averages by key
   */
  def calculateAverageOfAverages(data: RDD[(Int,Double)]): Double =
    data.map(t => t._2).sum()/data.count()


  /**
   * Maps each (key,Value) to (Key,Deviation)
   * with the deviation=|value-ref| being the deviation of each average rating per key from the reference ref.
   * @param dataByKey RDD[(k:Int,v:Double)] with k being user or item and v the average rating.
   * @param ref the reference to use to compute the deviation.
   * @return RDD[(Int,Double)] key and normalized deviation.
   */
  def deviation(dataByKey: RDD[(Int,Double)], ref: Double): RDD[(Int,Double)] =
    dataByKey.map( t => (t._1,math.abs(t._2-ref)) )

  /**
   * Compute the ratio of number of items whose ratings deviates from the reference ref at most epsilon.
   * @param dataByKey RDD[(k:Int,v:Double)] with k being user or item and v the average rating.
   * @param ref the reference to use to compute the deviation.
   * @param epsilon the maximum deviation
   * @return Double
   */
  def ratioDeviated(dataByKey:RDD[(Int,Double)], ref:Double, epsilon:Double):Double = {
    val deviations = deviation(dataByKey,ref).map(t => if (t._2 > epsilon) 0 else 1)
    deviations.sum() / deviations.count()
  }

  /**
   * Scale the difference between x and r.
   * @param x Double, value to scale.
   * @param r Double, reference value.
   * @return Double, scaled value
   */
  def scale(x:Double, r:Double): Double = (x > r) match {
    case true => 5 - r
    case false => if (x == r) 1 else r-1
  }

  /**
   * Compute the normalized deviation of x compared to r.
   * @param x Double, The original value
   * @param r Double, The reference
   * @return Double, The normalized deviation of x from r.
   */
  def normalize(x:Double, r:Double): Double = {
    (x-r)/scale(x,r)
  }

  /**
   * Maps the baseline model on the training data X and testing data Y.
   * @param X RDD[(Rating)], Train Data
   * @param Y RDD[(Rating)], Test Data
   * @return RDD[(Rating,Double)], Ratings with corresponding predicted value.
   */
  def baseline(X:RDD[Rating],Y:RDD[Rating]): RDD[(Rating,Double)] = {

    def predict(metric:Double,avDev:Double): Double =
      metric + avDev*utils.scale(metric+avDev,metric)

    val userAvRating = calculateAverageBykey(X,"user")

    val itemAvDev = X.map(r=>(r.user,(r.item,r.rating))).leftOuterJoin(userAvRating)
                      .map(r=> utils.Rating(r._1,r._2._1._1, normalize(r._2._1._2, r._2._2.get)) )
                      .map(r=> (r.item,r.rating)).aggregateByKey((0.0,0))( //average deviation by item
                      (accumulator,element) => (accumulator._1+element, accumulator._2+1),
                      (accumulator1,accumulator2) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)
                    ).map(r => (r._1,r._2._1/r._2._2.toDouble))


    val joinedUserAve = Y.map(r => (r.user,(r.item,r.rating)))
                         .join(userAvRating) // RDD[(user,((item,rating),avRating)]
                         .map(r => (Rating(r._1,r._2._1._1,r._2._1._2),r._2._2 ))


    // joinedData: RDD[Rating,userAverage,ItemAvDeviation]
    val joinedData = joinedUserAve.map(r => (r._1.item, ((r._1.user,r._1.rating), r._2) ) )
                                      .leftOuterJoin(itemAvDev) // RDD[(Item, ( ( (user, rating), avRating ),  avDeviation ) )]
                                      .map(r => (Rating(r._2._1._1._1,r._1,r._2._1._1._2),r._2._1._2,r._2._2))

    val predictions = joinedData.map( r => (r._1, predict(r._2,r._3.getOrElse(0))))
    predictions
  }

  /**
   * Maps the custom model on the training data X and testing data Y.
   * @param X RDD[(Rating)], Train Data
   * @param Y RDD[(Rating)], Test Data
   * @param famousCriteria Long, the count of items of which we will consider an item famous.
   * @return RDD[(Rating,Double)], Ratings with corresponding predicted value.
   */
  def myModel(X:RDD[Rating],Y:RDD[Rating], famousCriteria:Long):RDD[(Rating,Double)] = {

    def sigmoid(x:Double,famousCriteria:Long) : Double = 1 / (1+math.exp(-x+famousCriteria))

    def predict(metric:Double,avDev:Double, weight:Double): Double =
      weight * ( (metric + avDev*utils.scale(metric+avDev,metric)) - 1) + 1

    val userAvRating = calculateAverageBykey(X,"user")

    // Items average deviations
    val itemAvDev = X.map(r=>(r.user,(r.item,r.rating))).leftOuterJoin(userAvRating)
      .map(r=> utils.Rating(r._1,r._2._1._1, normalize(r._2._1._2, r._2._2.get)) )
      .map(r=> (r.item,r.rating)).aggregateByKey((0.0,0))( //average deviation by item
      (accumulator,element) => (accumulator._1+element, accumulator._2+1),
      (accumulator1,accumulator2) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)
    ).map(r => (r._1,r._2._1/r._2._2.toDouble))


    // Joining on users Average Rating
    val joinedUserAve = Y.map(r => (r.user,(r.item,r.rating)))
      .join(userAvRating) // RDD[(user,((item,rating),avRating)]
      .map(r => (Rating(r._1,r._2._1._1,r._2._1._2),r._2._2 ))


    // joinedData: RDD[Rating,userAverage,ItemAvDeviation]
    val joinedData = joinedUserAve.map(r => (r._1.item, ((r._1.user,r._1.rating), r._2) ) )
      .leftOuterJoin(itemAvDev) // RDD[(Item, ( ( (user, rating), avRating ),  avDeviation ) )]
      .map(r => (Rating(r._2._1._1._1,r._1,r._2._1._1._2),r._2._1._2,r._2._2))

    // Famous movies
    val ratedItemsCount = X.map(r => r.item).countByValue()
    val itemsWeightsMap = ratedItemsCount.map(r => (r._1, sigmoid(r._2, famousCriteria)))
    // (item, weight)
    val itemWeights = X.groupBy(r=>r.item).map(r => (r._1, itemsWeightsMap(r._1)))


    // RDD[._1(item, ._2(._1(._1user, ._2rating, ._3userAverage, ._4avDeviation), ._2weight ) )]
    val fullJoinedData = joinedData.map( r => (r._1.item,(r._1.user, r._1.rating, r._2, r._3.get )) ).join(itemWeights)

    // Output
    val predictions = fullJoinedData.map( r => (Rating(r._2._1._1,r._1,r._2._1._2), predict(r._2._1._3,r._2._1._4, r._2._2 )))
    predictions

  }

  /**
   * Given training data X and testing data Y, maps the model using the method "method" on the data.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @param method String
   * "global" -> Global average method
   * "user" -> user average method
   * "item" -> item average method
   * "baseline" -> baseline model
   * @return RDD[(Rating,Double)] data along with predictions
   */
  def computePredictions(X:RDD[Rating],Y:RDD[Rating],method:String): RDD[(Rating,Double)] = {
    method.toLowerCase match {
      case "global" => { val average = computeGlobalAverage(X)
                          Y.map(r => (r,average))
      }
      case "user" => {val averagePerUser = utils.calculateAverageBykey(X,"user")
                      val testByUser = Y.map(r => (r.user,(r.item,r.rating)))
                        .leftOuterJoin(averagePerUser)
                      testByUser.map(r => (Rating(r._1,r._2._1._1,r._2._1._2),r._2._2.get))
      }
      case "item" => {val globalAverage = computeGlobalAverage(X)
                      val averagePerItem = utils.calculateAverageBykey(X,"item")
                      val testByItem = Y.map(r => (r.item,(r.user,r.rating)))
                        .leftOuterJoin(averagePerItem)
                      testByItem.map(r => ( Rating(r._2._1._1, r._1,r._2._1._2), r._2._2.getOrElse(globalAverage) ) )

      }
      case "baseline" => baseline(X,Y)
    }
  }

  /**
   * Given training data X and testing data Y, maps the model using the method "method" and compute
   * the corresponding average Mean Absolute Error.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @param method String
                           * "global" -> Global average method
                           * "user" -> user average method
                           * "item" -> item average method
                           * "baseline" -> baseline model
   * @return the Mean Absolute Error
   */
  def computeMae(X:RDD[Rating],Y:RDD[Rating],method:String): Double = {
    val predictions = computePredictions(X,Y,method.toLowerCase)
    predictions.map(r=>math.abs(r._1.rating-r._2)).reduce(_+_) / Y.count.toDouble
  }

  /**
   * @param data output of model predictions
   * @param movieTitles items names
   * @return List of top 5 movies base on predicted ratings
   */
  def top5Movies(data:RDD[(Rating,Double)], movieTitles:RDD[(Int,String)]):Array[List[Any]] = {
    val top5Movies = data.map(r => (r._1.item, r._2)).join(movieTitles).map(r => (r._1, r._2._1, r._2._2))
                            // Sorting descending on ratings and ascending on the items
                          .sortBy(e => (e._2, -e._1), false)
                          .take(5)
    for (l <- top5Movies.map(r => List(r._1, r._3, r._2)).array) yield l
  }

  /**
   * Compute the execution time for different methods
   * @param X RDD[Ratinng]
   * @param Y RDD[Rating]
   * @return Map[method:String,(min:Double,max:Double,average:Double,stdDev:Double)]
   *         with method in ("Global","User","Item","Baseline")
   */
  def executionTimes(X:RDD[Rating],Y:RDD[Rating]): Map[String,(Double,Double,Double,Double)] ={

      def time(method:String): List[Double] = {
        var methodTime: List[Double] = Nil
        for (i <- 1 to 10) {
          val start = System.nanoTime() / 1e3
          // We use sum to trigger the evaluation of the RDD
          computePredictions(X, Y, method.toLowerCase).map(r=>r._2).sum()
          val end = System.nanoTime() / 1e3
          methodTime = (end - start) :: methodTime
        }
        methodTime
      }

      def metrics(l:List[Double]): (Double,Double,Double,Double) = {
        val average = l.sum / l.length
        val stdDev = math.sqrt(l.map(e => math.pow(e-average,2)).sum / l.length)
        (l.min, l.max, average,stdDev)
      }

    Map(  "Global" -> metrics(time("Global")),
          "User" -> metrics(time("user")),
          "Item" -> metrics(time("item")),
          "Baseline" -> metrics(time("baseline"))
    )
    }


  }


