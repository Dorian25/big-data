package datacloud.spark.core.lastfm

import org.apache.spark._
import org.apache.spark.rdd.RDD


object HitParade {
  case class TrackId(id : String) 
  case class UserId(id : String)
  
  def loadAndMergeDuplicates(context:SparkContext, url_in:String) : RDD[((UserId,TrackId),(Int,Int,Int))] = {

    val data_fm = context.textFile(url_in)
                           .map(line => line.split(" "))
                           .map(arr => ((UserId(arr(0)),TrackId(arr(1))),(arr(2).toInt,arr(3).toInt,arr(4).toInt)))
                           .reduceByKey((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3))
    return data_fm                    
  }
  
  def hitparade(rdd: RDD[((UserId,TrackId),(Int,Int,Int))]) : RDD[TrackId] = {
    val nb_listener = rdd.map(t => if (t._2._1+t._2._2 > 0) (t._1._2,1) else (t._1._2,0))
                         .reduceByKey(_+_)
                                            
    val score = rdd.map(t => (t._1._2,t._2._1+t._2._2-t._2._3))
                   .reduceByKey(_+_)
                   
    // le signe "-" permet de trier par ordre décroissant
    val res = nb_listener.join(score)
                         .sortBy(x=> (-x._2._1,-x._2._2,x._1.id))

    return res.map(t => t._1)
  }
}