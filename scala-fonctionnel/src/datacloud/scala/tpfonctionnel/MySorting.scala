package datacloud.scala.tpfonctionnel

import scala.math.Ordering

object MySorting {
  def isSorted[A](tab:Array[A],f:(A,A)=> Boolean): Boolean = {
    var res = true
    for (i <- 0 until tab.size) {
      if (i+1 != tab.size) {
        res = res && f(tab(i),tab(i+1))
      }
    }
    return res
  }
      
  def ascending[T](implicit order:Ordering[T]):(T,T)=> Boolean = {
    val f = (a:T, b:T) => if (order.compare(a, b) < 0 || order.compare(a,b) == 0) true else false 
    return f
  }
  
  def descending[T](implicit order:Ordering[T]):(T,T)=> Boolean = {
    val f = (a:T,b:T) => if (order.compare(a, b) > 0 || order.compare(a,b) == 0) true else false
    return f
  }
}