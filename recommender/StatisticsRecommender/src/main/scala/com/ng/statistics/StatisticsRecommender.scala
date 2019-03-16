package com.ng.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//创建样例类
//统计推荐只需要movie和rating数据
case class Movie(val mid: Int, val name: String, val desc: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

//创建MongoDB配置样例类
case class MongoConfig(val uri: String, val db: String)

//定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义一个电影类别推荐对象
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val mONGODB_MOVIE_COLLECTION = "Movie"
  //统计表的名称
  //历史热门电影表
  val RATE_MORE_MOVIES = "RateMoreMOvies"
  //近期热门电影表
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  //电影平均评分表
  val AVERAGE_MOVIES = "AverageMovies"
  //电影类别top10表
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    //用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://psy835:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //加入隐式转换
    import spark.implicits._

    //声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //从mongodb加载数据进来
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", mONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建一张叫rating的临时表
    ratingDF.createOrReplaceTempView("ratings")

    //不同的统计推荐结果
    //（1）统计所有历史数据中每个电影的评分数
    //数据结构 -》 mid，count
    val rateMoreMoviesDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid")

    rateMoreMoviesDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计以月为单位拟每个电影的评分数
    //数据结构 -》 mid，count，time

    //创建一个日期格式化函数
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个UDF函数，用于将timestamp转换成年月格式 1260759144000  => 201605
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的时间转换成年月的格式，并且去掉uid
    val ratingOfYearMonth: DataFrame = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")

    //将新的数据集注册成为一张表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    //提取出按照年月和mid分组的评分数据统计
    val rateMoreRecentlyMovies: DataFrame = spark.sql("select mid,count(mid) as count, yearmonth from ratingOfMonth group by yearmonth,mid")

    rateMoreRecentlyMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()
  }
}
