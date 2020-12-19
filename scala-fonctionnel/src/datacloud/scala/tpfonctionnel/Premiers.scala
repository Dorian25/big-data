package datacloud.scala.tpfonctionnel

import scala.math.pow

object Premiers {
  def premiers(n: Int): List[Int] = {
    var l = List.range(2, n)
    for (i <- l) l = l.filter(x => x == i || x % i != 0)
    return  l
  }
  
  def premiersWithRec(n: Int): List[Int] = {
    def f(l: List[Int]): List[Int] = {
      if (pow(l.head,2) > l.last)
        return l
      else 
        return List(l.head) ++ f(l.tail.filter(x => x%l.head != 0))
    }
    f(List.range(2,n))
  }
}