package datacloud.scala.tpfonctionnel

object Statistics {
  def average(notes: List[(Double, Int)]): Double = {
    val s = notes.map(x => (x._1 * x._2, x._2)).reduce((a,b)=>(a._1+b._1,a._2+b._2))
    return s._1 / s._2
  }
}