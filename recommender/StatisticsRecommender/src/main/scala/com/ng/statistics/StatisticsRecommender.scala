package com.ng.statistics

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//创建样例类
//统计推荐只需要movie和rating数据
case class Movie( mid: Int, name: String, descri: String, timeLong: String, issue: String,
                  shoot: String, language: String, genres: String, actors: String, derectors: String
                )

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

//创建MongoDB配置样例类
case class MongoConfig(uri: String, db: String)

//定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义一个电影类别推荐对象
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  //统计表的名称
  //历史热门电影表
  val RATE_MORE_MOVIES = "RateMoreMovies"
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
      .option("collection", MONGODB_MOVIE_COLLECTION)
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

    //统计每个电影的平均得分
    val averageMoviesDF: DataFrame = spark.sql("select mid,avg(score) as avg from ratings group by mid")

    averageMoviesDF.write
        .option("uri",mongoConfig.uri)
        .option("collection",AVERAGE_MOVIES)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    //统计每个电影类别中评分最高的10个电影
    //movie和评分做inner连接
    val movieWithScore: DataFrame = movieDF.join(averageMoviesDF, Seq("mid", "mid"))

    //所有的电影类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    //将电影类别转换成RDD
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)
    //得到genres和movieRow的笛卡儿积，计算电影类别top10
    val genresTopMovie: DataFrame = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        //用bool判断条件进行过滤
        case (genres, row) => row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase())
      }
      .map { //对数据集进行简化，只需要genres,(mid,avg)，把mid和avg包装成元祖，方便之后聚合
        // 将整个数据集的数据量减小，生成RDD[String,Iter[mid,avg]]
        case (genres, row) => {
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }.groupByKey()
      .map {
        case (genres, items) =>
          GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }.toDF()

    // 输出数据到MongoDB
    genresTopMovie.write
        .option("uri",mongoConfig.uri)
        .option("collection",GENRES_TOP_MOVIES)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
    spark.stop()
  }
}
