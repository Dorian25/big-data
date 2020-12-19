package datacloud.spark.streaming.pi

import org.apache.spark._
import org.apache.spark.streaming
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object PiAtTime extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
  val conf = new SparkConf().setAppName("PiAtTime").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val points = lines.map(_.split(" "))
                    .map(t => (t(0).toDouble,t(1).toDouble))
                    
  //(in,1)
  // if in==1 points in cercle else points out cercle
  val in_out_pts = points.map(p => if (p._1*p._1 + p._2*p._2 < 1) (1.0,1.0) else (0.0,1.0))
  val nbInTotal = in_out_pts.reduce((a,b) => (a._1+b._1,a._2+b._2))
  val ratio = nbInTotal.map(x => 4  * x._1 / x._2)
  
  ratio.print()
  ssc.start()
  ssc.awaitTermination()
} 