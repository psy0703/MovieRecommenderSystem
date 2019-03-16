package com.ng.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Movie数据结构
  *
  * 260                                             // 电影ID
  * Star Wars: Episode IV - A New Hope (1977)       // 电影名称
  * Princess Leia is captured and held hostage ...  // 描述
  * 121 minutes                                     // 时长
  * September 21, 2004                              // 发行日期
  * 1977                                            // 拍摄年份
  * English                                         // 语言种类
  * Action|Adventure|Sci-Fi                         // 类型
  * Mark Hamill|Harrison Ford|Carrie Fisher|Peter ...   // 演员表
  * George Lucas                                    // 导演
  *
  */
case class Movie(mid:Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String
                )

/**
  * Rating数据结构
  *
  * 1,31,2.5,1260759144
  *
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * Tag数据结构
  *
  * 15,1955,dentist,1193435061
  *
  */
case class Tag(uid: Int, mid: Int, tags: String, timestamp: Int)

// 封装mongodb和es的配置，为样例类
case class MongoConfig(uri:String, db:String)
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)

object DataLoader2 {

  // 以window下为例，需替换成自己的路径，linux下为 /YOUR_PATH/resources/movies.csv
  val MOVIE_DATA_PATH = "E:\\IdeaProjects\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "E:\\IdeaProjects\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "E:\\IdeaProjects\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.192.136:9200",
      "es.transportHosts" -> "192.168.192.136:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )

    // 先创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建一个spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据，并转换成DataFrame
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(item => {
      // 通过^分割的字段，取出来
      val attr = item.split("\\^")
      // 把item换装成Movie
      Movie(attr(0).toInt, attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,
        attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item =>{
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    //将tagRDD装换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 将数据保存到Mongo DB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // 将数据保存到es
    storeDataInES()

    // 关闭spark
    spark.close()
  }

  // 保存数据到mongodb的实现
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    // 新建一个mongodb连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果已有表，那么删掉
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 把数据写入对应的表
    movieDF.write
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .option("uri", mongoConfig.uri)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据表建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    // 关闭连接
    mongoClient.close()
  }

  def storeDataInES(): Unit ={

  }

}
