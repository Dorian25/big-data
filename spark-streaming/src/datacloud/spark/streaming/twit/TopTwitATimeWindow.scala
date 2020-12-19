package datacloud.spark.streaming.twit

import org.apache.spark._
import org.apache.spark.streaming
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes


object TopTwitATimeWindow extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
  val conf = new SparkConf().setAppName("TopTwitAtTime").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("/tmp/toptwit/")
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val hashtags = lines.flatMap(line => line.split(" "))
                      .filter(word => word.startsWith("#"))
                      .map(h => (h,1))
  
  val x = Seconds(7)
  val y = Seconds(14)
                      
  //hashtags.reduceByKeyAndWindow(_+_, _-_, windowDuration, slideDuration, numPartitions, filterFunc)
  val count_hashtag = hashtags.reduceByKeyAndWindow(_+_, _-_, y, x)
  val top_hashtag = count_hashtag.transform(rdd => {rdd.sortBy(_._2, false)})
  
  top_hashtag.print()
  ssc.start()
  ssc.awaitTermination()  
  
}