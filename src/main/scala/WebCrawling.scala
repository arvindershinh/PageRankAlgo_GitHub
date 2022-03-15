import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object WebCrawling extends  App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  def flatUrlHtml(url: String, words: Seq[String]): Seq[(String, String)] = {
    words.map(x => (url, x))
  }

  def computeContribs(sink_urls: List[String], src_rank: Double): List[(String, Double)] =
  {
    for (sink_url <- sink_urls) yield (sink_url, src_rank / sink_urls.length)
  }

  val urlsRDD1 = spark.sparkContext.wholeTextFiles("WebSites")


  val urlsRDD2 = urlsRDD1.map({case (a,b) => (a.split('/').last.split('.')(0), b)})
  val urlsRDD3 = urlsRDD2.mapValues(x => new Html(x))

//  Title Parsing
  val urlsRDD8 = urlsRDD3.mapValues(x => x.parseOutTitle())

//  Keyword Parsing
  val urlsRDD4 = urlsRDD3.mapValues(x => x.parseOutKeyWords())

//  Build inverted index
  val urlsRDD5 = urlsRDD4.flatMap({case (a,b) => flatUrlHtml(a,b)})
  val urlsRDD6 = urlsRDD5.map({case (a,b) => (b,a)})
  val urlsRDD7 = urlsRDD6.groupByKey()

//  Link Parsing
  val adjacentUrls = urlsRDD3.mapValues(x => x.parseOutLinks())

//  Page Rank calculation

  //  Iteration0
  val ranks0: RDD[(String, Double)] = adjacentUrls.map({case(src, sinks) => (src, 1.0)})
  val contribs0 = adjacentUrls.join(ranks0).flatMap({case (_, sinks_rank) => computeContribs(sinks_rank._1, sinks_rank._2)})
  //  Iteration1
  val ranks1 = contribs0.reduceByKey(_+_).mapValues(rank => rank*0.85 + 0.15)
  val contribs1 = adjacentUrls.join(ranks1).flatMap({case (_, sinks_rank) => computeContribs(sinks_rank._1, sinks_rank._2)})
  //  Iteration2
  val ranks2 = contribs1.reduceByKey(_+_).mapValues(rank => rank*0.85 + 0.15)

//  print(adjacentUrls.collect().toList.head)
  ranks2.foreach(print)
}
