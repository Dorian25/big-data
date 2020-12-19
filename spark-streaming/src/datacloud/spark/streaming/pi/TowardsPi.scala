package datacloud.spark.streaming.pi

import org.apache.spark._
import org.apache.spark.streaming
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object TowardsPi extends App {
  
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
  val conf = new SparkConf().setAppName("TowardsPi").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("/tmp/pi")
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val points = lines.map(_.split(" "))
                    .map(t => (t(0).toDouble,t(1).toDouble))
                    
  //(1,(in_or_out,1))
  // if in==1 "points in cercle" else "points out cercle"
                    
  //on la meme clé =1 pour chaque pt, pour pouvoir les regrouper par la suite en faisant le update
  ssc.checkpoint("/tmp/toptwit/")
  val in_out_pts = points.map(p => if (p._1*p._1 + p._2*p._2 < 1) (1,(1.0,1.0)) else (1,(0.0,1.0)))
  
  def update(vals:Seq[(Double,Double)], state:Option[(Double,Double)]):Option[(Double,Double)] = {
    val tmp = vals.reduce((a,b) => (a._1+b._1,a._2+b._2))
    state match {
      case None => Some(tmp)
      case Some(cumulatif) => Some((tmp._1+cumulatif._1,tmp._2+cumulatif._2))
    }
  }
  
  val nbInTotal = in_out_pts.updateStateByKey(update)
  val ratio = nbInTotal.map(x => 4  * x._2._1 / x._2._2)
  
  ratio.print()
  ssc.start()
  ssc.awaitTermination()
}