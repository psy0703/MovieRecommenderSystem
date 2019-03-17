package com.ng.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 模型评估和参数选取
  *
  * @author cloud9831
  *         @2019-03-17 22:48
  */
object ALSTrainer {


  def main(args: Array[String]): Unit = {
    //配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD: RDD[Rating] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd //转换成RDD,yinwei ALS输入数据的要求时RDD
      .map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    //将一个ＲＤＤ随机切分为两个ＲＤＤ，用以划分训练集和测试集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8,0.2))

    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    //把trainRDD传入模型，训练模型，用testRDD计算RMSE，最终根据不同的参数选对应的RMSE。得到最优参数
    adjustALSParams(trainingRDD,testingRDD)

    //关闭spark
    spark.close()
  }

  //计算RMSE的函数
  def getRMSE(model: MatrixFactorizationModel, trainData: RDD[Rating]) = {
    val userMovies: RDD[(Int, Int)] = trainData.map( item => (item.user,item.product))
    val predictRating: RDD[Rating] = model.predict(userMovies)
    //构建希望的数据结构 （uid，mid）,rating
    val real: RDD[((Int, Int), Double)] = trainData.map(item => ((item.user,item.product),item.rating))
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user,item.product),item.rating))

    //计算RMSE
    //用real和predict做inner join，得到（uid，mid），（real，predict）
    sqrt(real.join(predict).map{case((uid,mid),(real,pre)) =>
    //真实值和预测值之间的差值
      val err: Double = real - pre
      err * err
    }.mean()
    )
  }

  //adjustALSParams方法是模型评估的核心，输入一组训练数据和测试数据，输出计算得到最小RMSE的那组参数
// 输出最终的最优参数
  def adjustALSParams(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]) = {
  //根据不同的参数选取，迭代计算模型，分别求出当前的RMSE
  // 这里指定迭代次数为5，rank和lambda在几个值中选取调整
  for(rank <- Array(100,200,250);lambda <- Array(1,0.1,0.01,0.001))
    yield {
      //用trainData来训练模型
      val model: MatrixFactorizationModel = ALS.train(trainingRDD, rank, 5, lambda)
      val rmse = getRMSE(model,testingRDD)
      (rank,lambda,rmse)
    }
  //按照rmse排序
  println()
}
}
