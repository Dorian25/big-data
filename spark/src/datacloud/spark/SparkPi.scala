package datacloud.spark

import scala.math.random
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkPi extends App {  
  // permet de se limiter à un affichage simple du log 
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
  
  val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val spark = new SparkContext(conf)
  val slices = if (args.length > 0) args(0).toInt else 5
  val n = math.min(100000L * slices, Int.MaxValue).toInt
  val count = spark.parallelize(1 until n, slices).map { i =>
    val x = random * 2 -1
    val y = random * 2 -1
    if (x*x + y*y < 1) 1 else 0
  }.reduce(_ + _)
  println("Pi is roughly " + 4.0 * count / n)
  spark.stop()
}