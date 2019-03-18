package com.ng.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: MovieRecommenderSystem
  * Package: com.atguigu.streaming
  * Version: 1.0
  *
  * Created by wushengran on 2019/3/18 9:11
  */

// 建立一个单例的连接助手对象
object ConnHelper extends Serializable{
  // 定义redis和mongo连接对象
  lazy val jedis = new Jedis("psy835")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://psy835:27017/recommender"))
}


// 创建mongodb配置样例类
case class MongoConfig(uri:String, db:String)

// 定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义一个用户推荐对象
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义一个电影相似度列表对象
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommeder {

  // 定义常量和表名

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy835:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 广播电影相似度矩阵，为性能考虑
    // 数据结构: Map[Int, Map[Int, Double]]
    val simMoviesMatrix = spark.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        item => ( item.mid, item.recs.map(x => (x.mid, x.score)).toMap )
      }
      .collectAsMap()         // 把原则转换成map，想要的数据结构，方便后续按照key来查询

    val simMoviesMatrixBroadcast = sc.broadcast(simMoviesMatrix)

    // 创建到kafka的连接，首先创建一个kafkaPara
    val kafkaPara = Map(
      "bootstrap.servers" -> "psy835:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )

    // 产生评分数据流，原始数据 UID|MID|SCORE|TIMESTAMP
    val ratingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // 核心实时推荐流程，对每个数据流中的rdd做处理
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) =>

          println("rating data coming! >>>>>>>>>>>>>>>>>>>>>>>>>>>> " + (uid, mid, score, timestamp))

          // 1. 从redis获取当前电影的最近K次评分，保存成一个数组Array[(Int, Double)]
          val userRecentlyRatings = getUserRecentlyRatings( MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis )

          // 2. 获取跟当前电影相似的N个电影，并过滤掉已经看过的，作为备选电影列表, Array[(Int)]
          val simMovies = getTopSimMovies( MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadcast.value )

          // 3. 计算每一个备选电影的推荐优先级，得到当前用户的实时推荐列表, Array[(Int, Double)]
          val streamRecs = computeMovieScores( simMoviesMatrixBroadcast.value, userRecentlyRatings, simMovies )

          // 4. 把按照推荐优先级排序的推荐列表存入mongodb
          saveRecsToMongoDB( uid, streamRecs )

      }
    }

    // 启动Streaming
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()

  }

  /**
    *
    * @param k        最近评分的个数k
    * @param uid      用户id，谁的评分
    * @param jedis    redis连接对象
    * @return         最近评分列表Array[(uid, score)]
    */
  def getUserRecentlyRatings(k: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis里读取数据，redis里评分数据存储为一个list，key是uid:UID，value是 UID:SCORE
    jedis.lrange("uid:" + uid, 0, k-1).map{
      item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
    }.toArray
  }

  /**
    *
    * @param n            备选的相似电影数量
    * @param mid          当前电影ID
    * @param uid          当前用户ID
    * @param simMovies    电影相似度矩阵
    * @param mongoConfig  MongoDB的配置对象
    * @return             过滤后的备选电影列表 Array[(mid)]
    */
  def getTopSimMovies(n: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 从相似度矩阵中选取跟当前电影相似的电影列表
    val allSimMovies = simMovies(mid).toArray
    // 从rating表里取出用户已经看过的电影列表，利用到mongo client, Array[mid]
    val ratingExist = ConnHelper.mongoClient( mongoConfig.db )( MONGODB_RATING_COLLECTION ).find( MongoDBObject("uid"->uid) )
      .toArray.map{ item => item.get("mid").toString.toInt }

    // 过滤掉用户已经看过的电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1) ).sortWith(_._2>_._2).take(n).map(x=>x._1)
  }

  def computeMovieScores(simMovies: scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]], userRecentlyRatings: Array[(Int, Double)], candidateMovies: Array[Int]): Array[(Int, Double)] = {
    // 定义长度可变数组，用于保存每个一备选电影的基本推荐得分，(Int, Double)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义HashMap，用于保存每一个备选电影的增强因子和减弱因子，HashMap[Int, Int]
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for( candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings ){

      // 先拿到候选电影和已评分电影的相似度
      val simScore = getMovieSimScore( candidateMovie, userRecentlyRating._1, simMovies )

      if(simScore > 0.6){
        // 计算每一个已评分电影对于当前候选电影的基础推荐评分，(mid, score)添加到scores列表中去
        scores += ( (candidateMovie, simScore * userRecentlyRating._2) )

        // 计算增强因子和减弱因子
        if(userRecentlyRating._2 > 3){
          // 高分电影，增强因子计数器+1
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        }else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }

    // 根据候选电影mid做聚合，得到一组基础推荐得分Map( mid -> ArrayBuffer[(mid, score)] )，求均值，再按照公式加上因子
    scores.groupBy(_._1).map{
      case (mid, scoreLists) => ( mid, scoreLists.map(_._2).sum / scoreLists.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)) )
    }.toArray
  }

  def getMovieSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(sim) => sim.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("uid"->uid))
    streamRecsCollection.insert(MongoDBObject( "uid"->uid, "recs"-> streamRecs.map( x=>MongoDBObject("mid"->x._1, "score"->x._2) ) ))
  }

}

