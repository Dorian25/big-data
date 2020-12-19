package datacloud.scala.tpfonctionnel

object Counters {
  def nbLetters(l: List[String]): Int = {
    return l.flatMap(x => x.split(" ")).map(_.length()).reduce(_+_)
  }
}