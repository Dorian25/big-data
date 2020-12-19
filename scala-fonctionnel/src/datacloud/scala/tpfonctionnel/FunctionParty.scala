package datacloud.scala.tpfonctionnel

object FunctionParty {
  def curryfie[A,B,C](f:(A,B)=>C): A => B => C = {
    return (x:A) => (y:B) => f(x,y)
  }
  
  def decurryfie[A,B,C](f:A => B => C):(A,B) => C = {
    return (x:A,y:B)=> f(x)(y)
  }
  
  def compose[A,B,C](f:B=>C, g:A=>B): A => C = {
    return (x:A) => f(g(x))
  }
  
  def axplusb(a:Int,b:Int): Int=>Int = {
    val curry_add = curryfie((x:Int,y:Int) => x+y)
    val curry_mul = curryfie((x:Int,y:Int) => x*y)
    
    val affine = (x:Int) => compose(curry_add(b),curry_mul(a))(x)
 
    return affine
  }
}