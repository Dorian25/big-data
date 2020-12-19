package datacloud.scala.tpobject.vector

class VectorInt(val elements : Array[Int]) {
	
	def length():Int = {
		this.elements.length
	}

	def get(i:Int):Int = {
		if(i >=0 && i < this.length()) {
			this.elements(i)
		} else {
			println("Erreur d'index, i doit etre compris entre 0 et length-1")
			-1
		}
	}

	def tostring():String = {
		this.elements.mkString(" ")
	}

	override def equals(a:Any):Boolean= a match {
		// on test si l'éléments est 
		case x : VectorInt =>  this.elements.sameElements(x.asInstanceOf[datacloud.scala.tpobject.vector.VectorInt].elements)
		case _ => false
	}
	
	def +(other:VectorInt):VectorInt = {

		if(other.length == this.length){
			var add_elem : Array[Int] = new Array[Int](this.length)

			for(i <- 0 until other.length) {
				add_elem(i) = this.elements(i) + other.elements(i)
			}

			new VectorInt(add_elem)
		} else {
			new VectorInt(new Array[Int](0))
		}

	}

	def *(v:Int): VectorInt = {
		if(this.length > 0) {
			var prod_elem : Array[Int] = new Array[Int](this.length)

			for(i <- 0 until this.length) {
				prod_elem(i) = this.elements(i) * v
			}

			new VectorInt(prod_elem)

		} else {
			new VectorInt(new Array[Int](0))
		}
	}
 
	def prodDyadique(other:VectorInt):Array[VectorInt] = {
		// le vecteur determine le nombre de lignes de la matrice
		var prod_d : Array[VectorInt] = new Array[VectorInt](this.length)

		for(i <- 0 until this.length){
			prod_d(i) = other * this.elements(i)
		}
		prod_d
	}
}

object VectorInt {

	implicit def convert(tab:Array[Int]) : VectorInt = {
		new VectorInt(tab)
	}
}