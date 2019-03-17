package com.ng.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 基于隐语义模型的协同过滤推荐
  * 用户电影推荐矩阵
  * 电影相似度矩阵
  *
  * @author cloud9831 
  *         @2019-03-17 20:34 
  */
//声明样例类
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String,language: String, genres: String, actors: String, directors: String )
case class MovieRating(uid:Int,mid:Int,score:Double,timestamp:Int)
case class MongoConfig(uri:String,db:String)
// 标准推荐对象，mid,score
case class Recommendation(mid:Int,score:Double)
//用户推荐
case class UserRecs(uid:Int,recs:Seq[Recommendation])
//电影相似度列表对象（电影推荐）
case class MovieRecs(mid:Int,recs:Seq[Recommendation])

object OfflineRecommender {

  //定义表名常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //最终存入mongodb的推荐表名
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  //用户的最大推荐列表长度
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    //定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy835:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建spark session
    val sparkConf: SparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //读取mongodb中的业务数据
    val ratingRDD: RDD[(Int, Int, Double)] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd //转换成RDD，因为ALS输入的数据要求时RDD
      .map(rating => (rating.uid, rating.mid, rating.score)) //去掉时间戳
      .cache()  //考虑性能，设置缓存

    //用户的数据集RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    //电影数据集RDD
    val movieRDD: RDD[Int] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collectioin", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //创建训练数据集
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    //rank时模型中的隐语义因子的个数，iterations是迭代的次数，lambda是ALS的正则化参数
    val (rank,iterations,lambda) = (50,5,0.01)
    // 调用ALS算法训练隐语义模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    //计算用户推荐矩阵
    //用所有的uid和mid做笛卡儿积，得到一个空的评分矩阵
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    //model已训练好，把id传进去就可以得到预测评分列表RDD[Rating]（uid，mid，rating）
    val preRatings: RDD[Rating] = model.predict(userMovies)
    //预测评分按照uid做处理，然后保存为UserRecs
    val userRecs: DataFrame = preRatings.filter(_.rating > 0) //做过滤，筛选评分大于0的rating（小于0说明没看过）
      .map(rating => (rating.user, (rating.product, rating.rating))) //转换成期待的格式
      .groupByKey()
      .map { //做排序，提取操作，转换成最终想要的UserRecs结构
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //计算电影相似度矩阵
    //获取电影的特征矩阵，数据格式RDD[(scala.Int, scala.Array[scala.Double])]
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map { case (mid, features) => (mid, new DoubleMatrix(features)) }

    //计算笛卡儿积并过滤合并
    //电影和电影集合做笛卡儿积，所有的特征向量两两配对，计算相似度
    val movieRecs: DataFrame = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 } //条件过滤，排除自己跟自己做计算
      .map { case (a, b) =>
      val simScore = this.consinSim(a._2, b._2) //求a，b的余弦相似度
      (a._1, (b._1, simScore)) //构建想要得到的数据结构，方便之后根据mid做聚合，返回
    }.filter(_._2._2 > 0.6)  //过滤，只选取相似度大于0.6的写入表中
      .groupByKey()    //按照mid做group
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //关闭spark
    spark.close()
  }

  //计算两个电影之间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    return movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
