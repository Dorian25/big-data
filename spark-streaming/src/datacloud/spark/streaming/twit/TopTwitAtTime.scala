package datacloud.spark.streaming.twit

import org.apache.spark._
import org.apache.spark.streaming
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object TopTwitAtTime extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
  val conf = new SparkConf().setAppName("TopTwitAtTime").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val hashtags = lines.flatMap(line => line.split(" "))
                      .filter(word => word.startsWith("#"))
                      .map(h => (h,1))
                      
  val count_hashtag = hashtags.reduceByKey(_+_)
  val top_hashtag = count_hashtag.transform(rdd => {rdd.sortBy(_._2, false)})
  
  
  top_hashtag.print()
  ssc.start()
  ssc.awaitTermination()
}