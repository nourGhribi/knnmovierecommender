import breeze.linalg._
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map

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

  type GroupeData = scala.collection.Map[Int,Set[Int]]


  val MAE_BASELINE = 0.7669
  /**
   * Uncomment this part for spark session
  val spark = SparkSession.builder()
    .getOrCreate()
   */

  def round_double(d:Double): Double = "%.4f".format(d).toDouble


  case class KNN_result(predictions: CSCMatrix[Double], knn_mae: Double, knn_time: Double, pred_time: Double)
  case class Stats(min:Double,max:Double,avg:Double,stdDev:Double)
  case class Result(MAE:Double, knn_stats:Stats, pred_stats:Stats)



  //----------------------------------------------------------------
  // Milestone 3
  //----------------------------------------------------------------
  /**
   * Compute the normalized deviations of the users
   * @param X The data Matrix
   */
  def normalizedDev_M3(X:CSCMatrix[Double]):CSCMatrix[Double]={

    // X : nbUsers * nbMovies
    val nbUsers = X.rows
    val nbMovies = X.cols

    val normDevBuilder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)

    val unitVector = DenseVector.ones[Double](nbMovies) // nbMovies * 1

    val ratingsSumVector = X * unitVector // nbUsers * 1
    val nbMoviesRated = X.mapActiveValues(_ => 1.0) * unitVector // nbUsers * 1
    val averageRatingPerUser = ratingsSumVector / nbMoviesRated // nbUsers * 1

    val buildingNormDevs = for(((user,movie),value)<-X.activeIterator) normDevBuilder.add(user, movie, normalize(value, averageRatingPerUser(user)))

    normDevBuilder.result()
  }

  def preprocess(X:CSCMatrix[Double]) = {
    val nbUsers = X.rows
    val nbMovies = X.cols

    val normDev = normalizedDev_M3(X)

    val checkedRsBuilder = new CSCMatrix.Builder[Double](rows = nbUsers, cols = nbMovies)

    val unitVector = DenseVector.ones[Double](nbMovies) // nbMovies * 1
    val sqSumPerUser = normDev.mapActiveValues(math.pow(_,2)) * unitVector // NbUsers * 1
    val normsPerUser = sqSumPerUser.mapActiveValues(math.sqrt) // NbUsers * 1

    val building = for (((user, movie), value) <- normDev.activeIterator)
      checkedRsBuilder.add(user, movie, value / normsPerUser(user))

    checkedRsBuilder.result() // NbUsers * NbMovies
  }

  def parallel_knn(X:CSCMatrix[Double], sc:SparkContext, k:Int):CSCMatrix[Double]= {

    val nbUsers = X.rows
    val nbMovies = X.cols

    val checkedRs = preprocess(X) // Users * Movies

    val checkedRs_br = sc.broadcast(checkedRs)

    val checkedRst_br = sc.broadcast(checkedRs.t)


    /**
     * Computhe the knn similarities for user user and returns its similarity vector.
     * @param user to calculate its knn similarities
     * @return the similarity vector for the user
     */
    def topk(user:Int):SparseVector[Double]= {
      val simVector = SparseVector[Double](Array.tabulate(nbUsers)(_=>0.0))
      val S_Us = checkedRs_br.value * checkedRst_br.value(0 until checkedRs_br.value.cols,user).toDenseVector
      print(user)
      S_Us(user) = 0 // self similarity
      for (v <- argtopk(S_Us, k)) {
        simVector.update(v, S_Us(v))
      }
      simVector(user) = 0 // self similarity
      simVector
    }

    val topks = sc.parallelize(0 until nbUsers).map(topk).collect()

    val similarities_builder = new CSCMatrix.Builder[Double](rows = nbUsers, cols = nbUsers)
    // ------ Similarities ------
    for(u <- 0 until nbUsers){
      val uKsims = topks(u)
      for((v,s) <- uKsims.activeIterator)
        similarities_builder.add(v,u,s)
    }
    // --------------------------
    similarities_builder.result()
  }


  def predictionFunction(kSimilarities:  CSCMatrix[Double], X:  CSCMatrix[Double],Y: CSCMatrix[Double],
                         normDevs: CSCMatrix[Double],averageRatingPerUser: DenseVector[Double]
                         ,sc:SparkContext) = {

    val normDev_br = sc.broadcast(normDevs)
    val similarities_br = sc.broadcast(kSimilarities)
    val absSimilarities_br = sc.broadcast(kSimilarities.mapActiveValues(math.abs))
    val rated_br = sc.broadcast(X.mapActiveValues(_ => 1.0))
    val averageRatingPerUser_br = sc.broadcast(averageRatingPerUser)

    def predict_ui(u:Int,i:Int): (Int,Int,Double) = {
      val pred = predict(averageRatingPerUser_br.value(u), {

        val num = similarities_br.value(0 until X.rows, u).toDenseVector.t *
          normDev_br.value(0 until X.rows, i).toDenseVector
        val denum = absSimilarities_br.value(0 until X.rows, u).toDenseVector.t *
          rated_br.value(0 until X.rows, i).toDenseVector

        val weightedSum = num / denum

        if (weightedSum.isNaN) 0 else weightedSum

      })
      (u,i,pred)
    }


    val predictionsBuilder = new CSCMatrix.Builder[Double](rows = Y.rows, cols = Y.cols)

    val predictions = sc.parallelize(Y.activeIterator.map{case ((user,item),rating) => (user,item)}.toList)
      .map(x => predict_ui(x._1,x._2)).collect()


    for ((u,i,value) <- predictions) predictionsBuilder.add(u,i,value)
    predictionsBuilder.result()
  }


  /**
   * Using spark and parallelization
   * Compute the knn-similarities model using k, training data X and Testing data Y
   * @param X Training data
   * @param Y Testing data
   * @param k K to use for knn
   * @return Predictions, MAE, KNN-time and Predictions time
   */
  def spark_KNN_predictions(X:CSCMatrix[Double], Y:CSCMatrix[Double], k:Int, sc: SparkContext): KNN_result = {

    // The normalized deviations

    val normDevs = normalizedDev_M3(X) // NbUser * NbMovies: (u , items)

    val simStart = System.nanoTime

    // The knn similarities matrix¨

    val kSimilarities = parallel_knn(X, sc, k) // NbUsers * NbUsers: (v, u)

    val simsTime = (System.nanoTime- simStart)/pow(10.0,3)

    val similarities_br = sc.broadcast(kSimilarities)

    val itemsRated = X.mapActiveValues(_=>1.0) // Rating(u,movies)

    // Weighted sum (User Specific) Parallel

    /*val num = kSimilarities.t * normDevs // users * movies
    val itemsRated = X.mapActiveValues(_=>1.0) // Rating(u,movies)
    val absSims = kSimilarities.mapActiveValues(math.abs) // S(v,u)
    val denum = absSims.t * itemsRated*/


    // Avergae per user
    val unitVector = DenseVector.ones[Double](X.cols) // nbMovies * 1
    val ratingsSumVector = X * unitVector // nbUsers * 1
    val nbMoviesRated = X.mapActiveValues(_ => 1.0) * unitVector // nbUsers * 1
    val averageRatingPerUser = ratingsSumVector / nbMoviesRated // nbUsers * 1


    // Parallelize on Y
    val predStart = System.nanoTime

    val pred_matrix = predictionFunction(kSimilarities, X,Y, normDevs,averageRatingPerUser,sc)

    val predTime = (System.nanoTime- predStart)/pow(10.0,3)

    val mae = sum(abs(pred_matrix-Y))/Y.activeSize

    KNN_result(pred_matrix, mae, simsTime, predTime)

  }



  /**
   * Compute the similarities matrix for all users
   * @param X The data Matrix (NbUsers * NbMovies)
   * @return The similarities Matrix (NbUsers * NbUsers)
   */
  def compute_KNN_similarities(X:CSCMatrix[Double], k:Int):CSCMatrix[Double] = {
    val nbUsers = X.rows
    val nbMovies = X.cols

    val similarities_builder = new CSCMatrix.Builder[Double](rows = nbUsers, cols = nbUsers)

    // ------ Preprocessing -----
    val checkedRs = preprocess(X)
    // --------------------------

    // ------ Similarities -----
    for(user <- 0 until nbUsers){
      val denseSclice = checkedRs(user,0 until nbMovies).t.toDenseVector
      val simVector = checkedRs * denseSclice
      simVector(user) = 0 // self similarity
      for (v <- argtopk(simVector, k)) {
        similarities_builder.add(v, user, simVector(v))
      }
    }
    // --------------------------

    similarities_builder.result()
  }

  /**
   * Compute the knn-similarities model using k, training data X and Testing data Y
   * @param X Training data
   * @param Y Testing data
   * @param k K to use for knn
   * @return Predictions, MAE, KNN-time and Predictions time
   */
  def KNN_predictions(X:CSCMatrix[Double], Y:CSCMatrix[Double], k:Int): KNN_result = {

    // The normalized deviations
    val normDevs = normalizedDev_M3(X) // NbUser * NbMovies: (u , items)

    val simStart = System.nanoTime
    // The knn similarities matrix
    val kSimilarities = compute_KNN_similarities(normDevs,k) // NbUsers * NbUsers: (v, u)
    val simsTime = (System.nanoTime- simStart)/pow(10.0,3)

    val predStart = System.nanoTime
    // Weighted sum (User Specific)
    val num = kSimilarities.t * normDevs // users * movies
    val itemsRated = X.mapActiveValues(_=>1.0) // Rating(u,movies)
    val absSims = kSimilarities.mapActiveValues(math.abs) // S(v,u)
    val denum = absSims.t * itemsRated

    // Avergae per user
    val unitVector = DenseVector.ones[Double](X.cols) // nbMovies * 1
    val ratingsSumVector = X * unitVector // nbUsers * 1
    val nbMoviesRated = X.mapActiveValues(_ => 1.0) * unitVector // nbUsers * 1
    val averageRatingPerUser = ratingsSumVector / nbMoviesRated // nbUsers * 1

    val predictionsBuilder = new CSCMatrix.Builder[Double](rows = Y.rows, cols = Y.cols)

    var mae=0.0
    for(((u,i),value)<-Y.activeIterator){
      val pred = predict(averageRatingPerUser(u),{val weightedSum = num(u,i)/denum(u,i)
        if(weightedSum.isNaN) 0 else weightedSum
      })
      // Add predictions
      predictionsBuilder.add(u,i,pred)
      mae += math.abs(value-pred)
    }
    val predTime = (System.nanoTime- predStart)/pow(10.0,3)
    KNN_result(predictionsBuilder.result(), mae/Y.activeIterator.size, simsTime, predTime)
  }

  def time_stats(l:List[Double]):Stats = {
    val average = l.sum / l.length
    val stdDev = math.sqrt(l.map(e => math.pow(e-average,2)).sum / l.length)
    Stats(l.min, l.max, average,stdDev)
  }

  def compute_results(train:CSCMatrix[Double], test:CSCMatrix[Double], ks:List[Int])= {
    var knn_times: List[Double] = Nil
    var pred_times: List[Double] = Nil
    var maes: Map[Int,Double] = Map[Int,Double]()
    var results: Map[Int, Result] = Map[Int, Result]()

    for(k<-ks) {
      maes += (k -> KNN_predictions(train, test, k).knn_mae)
    }

    for(k<-ks){
      //Compute times
      for (i <- 1 to 5) {
        val knn_pred_results = KNN_predictions(train, test, k)
        knn_times = knn_pred_results.knn_time :: knn_times
        pred_times = knn_pred_results.pred_time :: pred_times
      }
      results+=( k -> Result( maes(k), time_stats(knn_times), time_stats(pred_times) ) )
    }
    results
  }


  //----------------------------------------------------------------
  // Milestone 2
  //----------------------------------------------------------------

  /**
   * Compute the items rated by each user or the users who rated each item.
   * @param X Train data
   * @return The Map of the set of items per user or set of users per item.
   */
  def group(X: RDD[utils.Rating], method: String): GroupeData = {
    method.toLowerCase() match {
      case "items" =>  X.groupBy(_.user).mapValues(_.map(_.item).toSet).collectAsMap()
      case "users" =>  X.groupBy(_.item).mapValues(_.map(_.user).toSet).collectAsMap()
    }

  }

  /**
   * Computes the normalized deviations of data.
   * @param X Train data
   * @return RDD[(u,i,rhat_ui)]
   */
  def normalizedDev(X:RDD[Rating]): RDD[(Int,Int,Double)] = {

    val userAverages = calculateAverageBykey(X,"user")

    X.map(r => (r.user,(r.item,r.rating))).leftOuterJoin(userAverages)
      .map(r => (r._1,r._2._1._1,normalize(r._2._1._2,r._2._2.get)) )
  }

  /**
   * Preprocess the data (Checked Rs)
   * @param X Train Data
   * @return (Map[(u,i), r_checked_ui]], RDD[(u,i, rhat_ui)], Map[i,U(i)] )
   */
  def preprocess_M2(X:RDD[Rating]): (Map[(Int, Int), Double], RDD[(Int,Int,Double)], GroupeData) = {

    def norm(ls: Iterable[Double]): Double = {
      math.sqrt(ls.map(e=>math.pow(e, 2)).sum)
    }

    val normDev = normalizedDev(X).cache()

    // The items rated by each user.
    // Map[u , I(u)]
    val itemsPerUser = group(X,"items")

    val norms = normDev.groupBy(_._1)
      .mapValues({ case ratings => norm(ratings.map(_._3)) })

    val joinedNorms = normDev.map{ case (u, i, rhat) => (u,(i,rhat))}
      .join(norms)

    val checkedRs = joinedNorms.map{ case (u,((i, normDev), norm_u) ) => ((u,i), if (itemsPerUser(u).contains(i)) normDev / norm_u else 0.0) }
      .collectAsMap()

    (checkedRs, normDev, itemsPerUser)

  }

  /**
   * Compute the adjusted cosine based similarities between all the users from the test data with the users present in the training data.
   * @param X Train data
   * @param Y Test data
   * @param knn whether to usee knn (true for knn) (false by default)
   * @param k to use for knn (0 by default)
   * @return Similarities along with the intersection size of items and normalized deviations.
   *         (Map[(u, v), (S_uv, |I(u) intersect I(v)|)], normDevs= RDD[(user,item, rhat_ui)])
   */
  def computeSimilarity(X:RDD[Rating],Y:RDD[Rating], method: String, knn:Boolean=false, k:Int=0): (Map[(Int, Int), (Double, Int)], RDD[(Int,Int,Double)]) = {

    val (checkedRs, normDevs, itemsPerUser) = preprocess_M2(X)

    //val uv = Y.groupBy(_.user).map(_._1).distinct().flatMap(u => itemsPerUser.keys.map(v => (u, v)))
    val uv = X.groupBy(_.user).map(_._1).distinct().flatMap(u => itemsPerUser.keys.map(v => (u, v)))

    // Compute all the similarities
    val s_uvs = method.toLowerCase() match {
      case "cosine" => {
        uv.map { case (u, v) => ((u, v), { val items = itemsPerUser(u).intersect(itemsPerUser(v))
          items.nonEmpty match {
            case false => (0.0, 0)
            case _ => (items.toList.map {
              i => checkedRs((u, i)) * checkedRs((v, i))
            }.sum, items.size)
          }
        })
        }
      }
      case "jaccard" => {
        uv.map { case (u, v) => ((u, v),{ val inter_size = itemsPerUser(u).intersect(itemsPerUser(v)).size
          (inter_size.toDouble / itemsPerUser(u).union(itemsPerUser(v)).size,
            inter_size)
        } )
        }

      }
    }


    // If knn keep only the k biggest similarities for each user u.
    if (knn) {
      val res = s_uvs.map{case ((u,v),(suv,intCount)) => (u,(v,suv,intCount))}
        .groupBy(_._1)
        .mapValues{ case svs => svs.map(_._2)}
        .mapValues{ case svs => svs.toSeq.sortWith(_._2 > _._2).take(k)}
        .flatMapValues(e => e)
        .map{case (u,(v,suv,intCount)) => ((u,v),(suv,intCount))}

      (res.collectAsMap(), normDevs)
    }
    else
      (s_uvs.collectAsMap(), normDevs)
  }

  /**
   * Given training data X and testing data Y, maps the model using the method "method" on the data.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @param method String
   * "cosine" -> Cosine similarity based model
   * "jaccard" -> Jaccard similarity based model
   * @return data along with predictions and intersections. (RDD[(Rating,Double)] , Map[(u,v), |I(u) intersect I(v)|])
   */
  def similarity_model(X:RDD[Rating], Y:RDD[Rating],method:String, knn:Boolean=false, k:Int=0):(RDD[(Rating,Double)], Map[(Int,Int), Int]) = {

    // The averages per user
    val userAvg = calculateAverageBykey(X,"user").collectAsMap()

    // The users who rated each item.
    // Map[i , U(i)]
    val usersItems = group(X,"users")

    // The similarities and the normalized deviations
    // (using cosine or jaccard metric) (with knn or without)
    // Map[(u,v), (s_uv, |I(u) inter I(v)|) ]] & RDD[(v, i, rhat_vi])]
    val (sims, normDev) = computeSimilarity(X,Y,method,knn,k)

    val intersectionsSize = sims.mapValues(_._2)

    val normDevs = normDev.map(dev => ((dev._1,dev._2),dev._3)).collectAsMap()

    // Weighted Sums
    val weightedSums = Y.map{case Rating(u,i,_) => (u,i)}.distinct()
      .map{case (u, item)=>((u,item),{val vs = usersItems.getOrElse(item,Set.empty[Int])
        val v_s = vs.filter(v=>v!=u)
        v_s.exists(v => sims.getOrElse((u, v),(0.0,0))._1 != 0.0) match {
          case false => (0.0,0.0)
          case true =>
            val num = v_s.toList.map(v =>
              sims.getOrElse((u,v),(0.0,0))._1 * normDevs(v,item)).sum
            val denum = v_s.toList.map(v => math.abs(sims.getOrElse((u,v),(0.0,0))._1)).sum
            (num, denum)
        }
      })
      }.mapValues{case (num,denum)=> if (denum!=0.0) num/denum else 0.0}
      .collectAsMap()



    // Joining on users Average Rating
    val joinedUserAve = Y.map(r => (r, userAvg(r.user)))

    // joinedData: RDD[Rating,userAverage,wSum]
    val joinedData = joinedUserAve.map{ case (r, usAve) => (r, usAve, weightedSums.getOrElse((r.user,r.item),0.0))}

    val predictions = joinedData.map{ case (rating, usAv, wSum) => (rating, predict(usAv, wSum))}

    (predictions, intersectionsSize)
  }


  /**
   * Given training data X and testing data Y, fit the model using the method "method" on the data.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @param method String:
   * "cosine" -> Cosine similarity based model,
   * "jaccard" -> Jaccard similarity based model
   * @return RDD[(Rating,Prediction)] data along with predictions and the intersections map Map[(u,v), |I(u) inter I(v)|].
   */
  def computePredictions_M2(X:RDD[Rating],Y:RDD[Rating],method:String): (RDD[(Rating,Double)], Map[(Int,Int),Int]) = {
    method.toLowerCase match {
      case "cosine"   => similarity_model(X,Y,"cosine")

      case "jaccard"  => similarity_model(X,Y,"jaccard")
    }
  }

  def calculateMae(predictions:RDD[(Rating,Double)]): Double = predictions.map(r=>math.abs(r._1.rating-r._2)).reduce(_+_) / predictions.count.toDouble

  /**
   * Given training data X and testing data Y, maps the knn model using the adjusted cosine metric
   * and computes the corresponding average Mean Absolute Error.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @return (k -> ( MAE_KNN(k), Map[(u,v), |I(u) inter I(v)|] ) )
   */
  def computeMae_M2_knn(X:RDD[Rating],Y:RDD[Rating]): Map[Int, (Double, Map[(Int,Int),Int])] = {
    val ks = List(10, 30, 50, 100, 200, 300, 400, 800, 943)

    val predictionsIntersections = (for (k<-ks) yield k -> similarity_model(X,Y,"cosine",knn = true,k=k) ).toMap

    predictionsIntersections.map{case (k, (predictions,intersections))=>
      k-> (calculateMae(predictions), intersections) }
  }

  /**
   * Given training data X and testing data Y, maps the model using the method "method" and compute
   * the corresponding average Mean Absolute Error.
   * @param X RDD[(Rating)]
   * @param Y RDD[(Rating)]
   * @param method String:
   * "cosine" -> Cosine similarity based model,
   * "jaccard" -> jaccard similarity based model
   * @return (the Mean Absolute Error, intersections of users rated items:Map[(u,v),|I(u) intersect I(v)|])
   */
  def computeMae_M2(X:RDD[Rating],Y:RDD[Rating],method:String): (Double,Map[(Int,Int),Int]) = {
    val (predictions, intersections) = computePredictions_M2(X,Y,method.toLowerCase)
    val mae = calculateMae(predictions)
    (mae,intersections)
  }
  //********************************************************************************************************************


  //----------------------------------------------------------------
  // Milestone 1
  //----------------------------------------------------------------

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
   * Compute prediction using formula:
   * metric + avDev*utils.scale(metric+avDev,metric)
   * @param metric
   * @param avDev
   * @return prediction
   */
  def predict(metric:Double,avDev:Double): Double =
    metric + avDev*utils.scale(metric+avDev,metric)

  /**
   * Maps the baseline model on the training data X and testing data Y.
   * @param X RDD[(Rating)], Train Data
   * @param Y RDD[(Rating)], Test Data
   * @return RDD[(Rating,Double)], Ratings with corresponding predicted value.
   */
  def baseline(X:RDD[Rating],Y:RDD[Rating]): RDD[(Rating,Double)] = {

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
   * "cosine" -> Cosine similarity based model
   * "jaccard" -> jaccard similarity based model
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
   * "cosine" -> Cosine similarity based model
   * "jaccard" -> jaccard similarity based model
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



  def metrics(l:List[Double]): (Double,Double,Double,Double) = {
    val average = l.sum / l.length
    val stdDev = math.sqrt(l.map(e => math.pow(e-average,2)).sum / l.length)
    (l.min, l.max, average,stdDev)
  }

  /**
   * Compute the execution time for different methods
   * @param X RDD[Ratinng]
   * @param Y RDD[Rating]
   * @param i: Number of iterations
   * @return Map[method:String,(min:Double,max:Double,average:Double,stdDev:Double)]
   *         with method in ("Global","User","Item","Baseline")
   */
  def executionTimes(X:RDD[Rating], Y:RDD[Rating], i:Int=10): Map[String,(Double,Double,Double,Double)] = {

    def time(method: String): List[Double] = {
      method.toLowerCase() match {
        case "similarity" => {
          var methodTime: List[Double] = Nil
          for (i <- 1 to i) {
            //spark.sqlContext.clearCache()
            val start = System.nanoTime() / 1e3
            computeSimilarity(X, Y, "cosine")
            val end = System.nanoTime() / 1e3
            methodTime = (end - start) :: methodTime
          }
          methodTime
        }

        case _ => {
          var methodTime: List[Double] = Nil
          for (i <- 1 to i) {
            //spark.sqlContext.clearCache()
            val start = System.nanoTime() / 1e3
            // We use sum to trigger the evaluation of the RDD
            computePredictions_M2(X, Y, method.toLowerCase)._1.map(r => r._2).sum()
            val end = System.nanoTime() / 1e3
            methodTime = (end - start) :: methodTime
          }
          methodTime
        }
      }

    }

    /* Milestone 1 times
    Map("Global" -> metrics(time("Global")),
        "User" -> metrics(time("user")),
        "Item" -> metrics(time("item")),
        "Baseline" -> metrics(time("baseline"))
    )*/

    // Milestone 2
    Map("cosine" ->  metrics(time("cosine")), "similarity" ->  metrics(time("similarity")))
  }


}