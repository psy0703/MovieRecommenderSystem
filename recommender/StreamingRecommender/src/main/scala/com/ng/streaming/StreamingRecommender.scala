package com.ng.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// 连接助手对象
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("psy835")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://psy835:27017/recommender"))
}

case class MongConfig(uri: String, db: String)

// 标准推荐
case class Recommendation(mid: Int, score: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {

  //最大用户评分数据数量
  val MAX_USER_RATINGS_NUM = 20
  //最大相似电影数量
  val MAX_SIM_MOVIES_NUM = 20
  //最终输出到mongdb的数据集名称
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  //从mongodb读取的数据集
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"


  //入口方法
  def main(args: Array[String]): Unit = {
    //配置信息
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy835:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    //创建一个SparkStreaming Context
    val ssc = new StreamingContext(sc, Seconds(2))
    //mongodb配置信息
    implicit val mongConfig = MongConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._


    // 广播电影相似度矩阵，为性能考虑
    //装换成为 Map[Int, Map[Int,Double]]
    val simMoviesMatrix = spark.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        item => (item.mid, item.recs.map(x => (x.mid, x.score)).toMap)
      }
      .collectAsMap() //把元祖转换为map，想要的数据结构，方便后续按照key来查询

    //将电影相似度矩阵作为广播变量
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "psy835:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    //UID|MID|SCORE|TIMESTAMP
    //产生评分流
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      case msg =>
        val attr: Array[String] = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    //核心实时推荐算法，对每个数据流中的rdd做处理
    ratingStream.foreachRDD {
      rdd =>
        rdd.map { case (uid, mid, score, timestamp) =>
          println(">>>>>>>>>>>>>>>>>>>>" + (uid, mid, score, timestamp))

          //1、从Redis中获取当前最近的M次电影评分，保存成一个数组Array[(Int,Double)]
          val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

          //2、获取跟当前电影P最相似的K个电影，并过滤掉已经看过的，作为备选的推荐列表
          val simMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

          //3、计算每一个待选电影的推荐优先级，得到当前用户的实时推荐列表
          val stramRecs: Array[(Int, Double)] = computeMovieScores(simMoviesMatrixBroadCast.value, userRecentlyRatings, simMovies)

          //4、把按照推荐优先级排序的推荐列表保存到MongoDB
          saveRecsToMongoDB(uid, stramRecs)
        }.count()
    }
    //启动Streaming程序
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()


  }

  /**
    * 将数据保存到MongoDB    uid -> 1,  recs -> 22:4.5|45:3.8
    *
    * @param uid        流式的推荐结果
    * @param streamRecs MongoDB的配置
    * @return
    */
  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongConfig: MongConfig): Unit = {
    //到StreamRecs的连接
    val streaRecsCollection: MongoCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streaRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" ->
      streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))
    ))
  }

  //使Redis的api支持Scala编程
  import scala.collection.JavaConversions._

  /**
    * 获取当前最近的M次电影评分
    *
    * @param num   最近评分的个数
    * @param uid   用户id，谁的评分
    * @param jedis jedis redis连接对象
    * @return 最近的评分列表[(uid,score)]
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis) = {
    //从用户的队列中取出num个评分,redis里评分数据存储为一个list，key是uid:UID,value是UID:SCORE
    jedis.lrange("uid: " + uid.toString, 0, num).map { item =>
      val attr: Array[String] = item.split("\\|")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前电影K个相似的电影
    *
    * @param num        相似电影的数量
    * @param mid        当前电影的ID
    * @param uid        当前的评分用户
    * @param simMovies  电影相似度矩阵的广播变量值
    * @param mongConfig MongoDB的配置
    * @return
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongConfig: MongConfig): Array[Int] = {

    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经观看过得电影
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map {
      item => item.get("mid").toString.toInt
    }
    //过滤掉已经评分过得电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2).take(num).map(x => x._1)
  }

  /**
    * 计算待选电影的推荐分数
    *
    * @param simMovies           电影相似度矩阵
    * @param userRecentlyRatings 用户最近的k次评分
    * @param topSimMovies        当前电影最相似的K个电影
    * @return
    */
  def computeMovieScores(simMovies: scala.collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int, Double)] = {

    //定义长度可变数组，用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //定义HashMap，用于保存每一个备选电影的增强因子数和减弱因子数，HashMap[Int,Int]
    val increMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings)
    //先拿到候选电影和已评分电影的相似度
    {
      val simScore: Double = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      if (simScore > 0.6) { //相似度大于0.6
        //计算每一个已评分电影对于当前电影的基础推荐评分，(mid,score)添加到scores列表中
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) { //用户评分大于3
          //高分电影，增强因子计数器加1
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        } else {
          //低分电影，减弱因子的计数器加1
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }
    //根据候选电影mid聚合，得到一组基础推荐得分Map(mid -> ArrayBuffer[(mid,score)],求平均值，再按照公式加上因子
    score.groupBy(_._1).map {
      case (mid, sims) =>
        (mid, sims.map(_._2).sum / sims.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray
  }

  /**
    * 获取当前电影之间的相似度
    *
    * @param simMovies       电影相似度矩阵
    * @param userRatingMovie 用户已评分的电影
    * @param topSimMovie     候选电影
    * @return
    */
  def getMoviesSimScore(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingMovie: Int, topSimMovie: Int) = {
    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  //取10的对数
  def log(i: Int) = {
    math.log(i) / math.log(10)
  }
}







































