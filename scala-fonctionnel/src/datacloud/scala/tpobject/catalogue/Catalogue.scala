package datacloud.scala.tpobject.catalogue

trait Catalogue {
	def getPrice(nom: String): Double
	def removeProduct(nom: String): Unit
	def selectProducts(min_price: Double, max_price: Double): Iterable[String]
	def storeProduct(nom: String, price: Double): Unit
}