package datacloud.scala.tpobject.catalogue

import datacloud.scala.tpobject.catalogue.Catalogue
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

// par défaut scala.collection.immutable (on ne pas rajouter/enlever d'éléments)
class CatalogueWithMutable(var map : Map[String,Double]) extends Catalogue {

	def this() = this(Map())

	override def getPrice(nom: String) : Double = {
		if(this.map.contains(nom)) {
			this.map(nom)
		} else {
			-1.0
		}
	}

	override def removeProduct(nom: String) : Unit = {
		if(this.map.contains(nom)) {
			this.map -= (nom)
		}
	}

	override def selectProducts(min_price: Double, max_price: Double)={
		var ls = new ListBuffer[String]()

		for((k,v)<- this.map) {
			if(v >= min_price && v <= max_price) {
         		ls += k
         	}
		}

		ls
	}

	override def storeProduct(nom: String, price: Double) : Unit = {
		this.map += (nom -> price)
	}

}
